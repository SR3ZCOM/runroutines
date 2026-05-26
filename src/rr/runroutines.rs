use std::{cell, ptr, mem, thread, time::{Instant, Duration}, collections::BinaryHeap, cmp::Ordering as CmpOrdering, io::{Error as IoError}};
use std::sync::{Arc, Once, OnceLock, atomic::{AtomicU64, Ordering}};
use crossbeam_deque::{Injector, Worker, Stealer};
use libc::{epoll_wait, epoll_event, epoll_create1, epoll_ctl, /*EPOLL_CTL_DEL,*/EPOLL_CTL_ADD, EPOLL_CTL_MOD, EPOLLIN, read, EAGAIN};

use crate::{slog_r, seprint_r, seprint_y, seprint_g, rr::{NC, GREEN, YELLOW, RED, /*DRED, CYAN, DCYAN, DYELLOW*/}};

type TaskFn = unsafe extern "C" fn(*mut ());

const STACK_SIZE: usize = 1024 * 1024;
const RR_THREADS_COUNT: usize = 4;
const WORKERS_COUNT_MAX: usize = 64;
const EVENTS_COUNT_MAX: usize = 64;
const RR_TICK_COUNT: i32 = 61;
const DEFAULT_TIMEOUT: i32 = 0;

#[derive(Debug)]
struct Registry {
  idle_mask: AtomicU64,
  handles: [OnceLock<thread::Thread>; WORKERS_COUNT_MAX],
}

#[allow(unused)]
#[repr(C)]
#[derive(Debug, Default)]
struct Context {
  rsp: usize,
  rip: usize,
  rdi: usize,
  rsi: usize,
  rbx: usize,
  rbp: usize,
  r12: usize,
  r13: usize,
  r14: usize,
  r15: usize,
}

#[allow(unused)]
#[repr(C)]
#[derive(Debug)]
pub struct RunroutineStruct {
  rsp: usize,
  stack: Vec<u8>,
}

#[derive(Copy, Clone)]
struct GPtr(*mut RunroutineStruct);

unsafe impl Send for GPtr {}
unsafe impl Sync for GPtr {}

#[allow(unused)]
#[derive(Debug, Clone)]
struct SharedP {
  id: usize,
  stealer: Stealer<GPtr>,
}

struct TimerTask {
  when: Instant,
  task: GPtr,
}

// ################################################################################################################################################################
impl Eq for TimerTask {}

impl PartialEq for TimerTask {
  fn eq(&self, other: &Self) -> bool { self.when == other.when }
}

impl Ord for TimerTask {
  fn cmp(&self, other: &Self) -> CmpOrdering { other.when.cmp(&self.when) }
}

impl PartialOrd for TimerTask {
  fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> { Some(self.cmp(other)) }
}
// ################################################################################################################################################################

static RUNTIME_INIT: Once = Once::new();
static GLOBAL_QUEUE: once_cell::sync::Lazy<Injector<GPtr>> = once_cell::sync::Lazy::new(Injector::new);
static REGISTRY: Registry = Registry { idle_mask: AtomicU64::new(0), handles: [const { OnceLock::new() }; WORKERS_COUNT_MAX] };

thread_local! {
  static SCHED_RSP: cell::Cell<usize> = const { cell::Cell::new(0) };
  static CURRENT_TASK: cell::Cell<*mut RunroutineStruct> = const { cell::Cell::new(ptr::null_mut()) };
  static WORKER: cell::Cell<*const Worker<GPtr>> = const { cell::Cell::new(ptr::null()) };
  static WORKER_STATE: cell::Cell<usize> = const { cell::Cell::new(usize::MAX) };

  static TICK_QUOTA: cell::Cell<i32> = const { cell::Cell::new(RR_TICK_COUNT) };
  static TIMERS: cell::RefCell<BinaryHeap<TimerTask>> = const { cell::RefCell::new(BinaryHeap::new()) };
  static EPOLL_FD: cell::Cell<i32> = const { cell::Cell::new(-1) };
}

#[macro_export]
macro_rules! rr_println {
  ($($arg:tt)*) => {{
    println!($($arg)*);
    // sleep_yield(1);
    arbit_yield();
  }};
}

// ################################################################################################################################################################
fn register_worker(id: usize) {
  WORKER_STATE.with(|w| w.set(id));

  let _ = REGISTRY.handles[id].set(thread::current());
}

// ################################################################################################################################################################
fn set_thread_idle() {
  let id = WORKER_STATE.with(|w| w.get());

  if id < WORKERS_COUNT_MAX {
    REGISTRY.idle_mask.fetch_or(1 << id, Ordering::Release);
  }
}

// ################################################################################################################################################################
fn set_thread_busy() {
  let id = WORKER_STATE.with(|w| w.get());

  if id < WORKERS_COUNT_MAX {
    REGISTRY.idle_mask.fetch_and(! (1 << id), Ordering::Release);
  }
}

// ################################################################################################################################################################
pub fn build_runtime(mut thcount: usize) {
  RUNTIME_INIT.call_once(|| {
    if 0 == thcount || thcount > RR_THREADS_COUNT {
      thcount = thread::available_parallelism().map(|n| n.get()).unwrap_or(RR_THREADS_COUNT);
    }
    let mut stealers = Vec::new();
    let mut workers = Vec::new();

    for i in 0..thcount {
      let worker = Worker::new_fifo();
      stealers.push(SharedP { id: i, stealer: worker.stealer() });
      workers.push(worker);
    }
    let shared_stealers = Arc::new(stealers);

    for id in 0..thcount {
      let local = workers.remove(0);
      let s_clone = Arc::clone(&shared_stealers);

      thread::spawn(move || {
        schedule(id, s_clone, local);
      });
    }
    eprintln!("✅ {}RUN_ROUTINES: BUILD_RUNTIME SPAWNED {} THREADS{} ⭐", GREEN, thcount, NC);
  });
}

// ################################################################################################################################################################
unsafe extern "C" { fn swap_stack(old_sp: *mut usize, new_sp: usize); }

// ################################################################################################################################################################
#[unsafe(no_mangle)]
unsafe extern "C" fn shim(func: TaskFn, data: *mut ()) {
  // eprintln!("shim: started");
  unsafe { func(data); }
  // eprintln!("shim: completed");
}

// ################################################################################################################################################################
pub fn async_read(fd: i32, buf: &mut [u8]) -> isize {
  loop {
    let n = unsafe {
      read(fd, buf.as_mut_ptr() as *mut _, buf.len())
    };

    if n >= 0 {
      return n;
    }
    let err = IoError::last_os_error();

    match err.raw_os_error() {
      Some(EAGAIN) /*| Some(EWOULDBLOCK) */ => {
        wait_for_fd(fd);

        continue;
      }
      _ => return -1,
    }
  }
}

// ################################################################################################################################################################
// pub fn sleep_yield(ms: u64) {
pub fn sleep_yield(duration: Duration) {
  let ptask = CURRENT_TASK.with(|c| c.get());

  if ptask.is_null() { return; }

  // let wake_at = Instant::now() + Duration::from_millis(ms);
  let wake_at = Instant::now() + duration;

  TIMERS.with(|timers| {
    timers.borrow_mut().push(TimerTask {
      when: wake_at,
      task: GPtr(ptask),
    });
  });
  eprintln!("{}sleep_yield: pushed: {:?}{}", YELLOW, wake_at, NC);

  unsafe {
    let psched_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if psched_rsp.is_null() { return; }

    swap_stack(&mut (*ptask).rsp, *psched_rsp);
  }
}

// ################################################################################################################################################################
pub fn arbit_yield() {
  let ptask = CURRENT_TASK.with(|c| c.get());

  if ptask.is_null() { return; }

  unsafe {
    WORKER.with(|w| {
      let ptr = w.get();

      if ! ptr.is_null() {
        (*ptr).push(GPtr(ptask));
        // eprintln!("arbit_yield: pushed_to_local: COUNT: {}", (*ptr).len());
      } else {
        GLOBAL_QUEUE.push(GPtr(ptask));
        // eprintln!("arbit_yield: pushed_to_global: COUNT: {}", GLOBAL_QUEUE.len());
      }
    });
    let psched_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if psched_rsp.is_null() { return; }

    swap_stack(&mut (*ptask).rsp, *psched_rsp);
  }
}

// ################################################################################################################################################################
pub fn wait_for_fd(fd: i32) {
  let ptask = CURRENT_TASK.with(|c| c.get());

  if ptask.is_null() { return; }

  let mut ev = epoll_event {
    events: EPOLLIN as u32,
    u64: ptask as u64,
  };

  unsafe {
    EPOLL_FD.with(|efd| {
      let epfd = efd.get();
      let rc = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &mut ev);

      if rc < 0 {
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &mut ev);
      }
    });
    let psched_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if ! psched_rsp.is_null() {
      swap_stack(&mut (*ptask).rsp, *psched_rsp);
    }
  }
}

// ################################################################################################################################################################
impl RunroutineStruct {
  pub fn add<T>(func: TaskFn, data: T) {
    const LOG_VALUE: &str = "RR_ADD";

    let mut stack = vec![0u8; STACK_SIZE];
    let top = unsafe { stack.as_mut_ptr().add(stack.len()) as *mut usize };
    let pdata = Box::into_raw(Box::new(data)) as *mut ();

    unsafe {
      let mut sp = top;

      sp = sp.offset(-1); *sp = 0;              // [Slot 12] Extra Padding for 16-byte parity
      sp = sp.offset(-1); *sp = 0;              // [Slot 11] Alignment Padding
      sp = sp.offset(-1); *sp = pdata as usize; // [Slot 10] RSI
      sp = sp.offset(-1); *sp = func as usize;  // [Slot 9]  RDI
      sp = sp.offset(-1);
      *sp = ret_to_sched as *const () as usize;
      sp = sp.offset(-1);
      *sp = shim as *const () as usize;

      for _ in 0..6 {
        sp = sp.offset(-1);
        *sp = 0;
      }
      GLOBAL_QUEUE.push(GPtr(Box::into_raw(Box::new(RunroutineStruct { rsp: sp as usize, stack }))));

      let mask = REGISTRY.idle_mask.load(Ordering::Acquire);

      if 0 != mask {
        let id = mask.trailing_zeros() as usize;

        if id < WORKERS_COUNT_MAX {
          let old_mask = REGISTRY.idle_mask.fetch_and(! (1 << id), Ordering::SeqCst);

          if 0 != (old_mask & (1 << id)) && let Some(t) = REGISTRY.handles[id].get() {
            t.unpark();
            seprint_g!("🔥 ID: {} ready_to_unpark 🚀", id);
          }
        }
      }
    }
  }
}

/// # Safety
// ################################################################################################################################################################
#[unsafe(no_mangle)]
unsafe extern "C" fn ret_to_sched() {
  unsafe {
    let ptask = CURRENT_TASK.with(|c| c.get());
    let psched_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if ptask.is_null() || psched_rsp.is_null() { return; }

    let mut discard_stack: usize = 0;

    (*ptask).rsp = 0;
    swap_stack(&mut discard_stack, *psched_rsp);
  }
}

// ################################################################################################################################################################
fn get_ready_timers(local: &Worker<GPtr>) -> u64 {
  TIMERS.with(|timers| {
    let mut timers = timers.borrow_mut();
    let now = Instant::now();

    while let Some(t) = timers.peek() {
      if t.when > now {
        break;
      }

      if let Some(tt) = timers.pop() {
        local.push(tt.task);
      } else {
        break;
      }
    }

    match timers.peek() {
      Some(next) => {
        next.when.saturating_duration_since(now).as_millis().max(1) as u64
      }
      None => 0,
    }
  })
}

// ################################################################################################################################################################
fn handle_event(epfd: i32, local: &Worker<GPtr>) {
  let mut events: [epoll_event; EVENTS_COUNT_MAX] = unsafe { mem::zeroed() };

  let n = unsafe {
    epoll_wait(epfd, events.as_mut_ptr(), EVENTS_COUNT_MAX as i32, DEFAULT_TIMEOUT)
  };

  for event in events.iter().take(n.max(0) as usize) {
    let task = GPtr(event.u64 as *mut RunroutineStruct);

    local.push(task);
  }
}

// ################################################################################################################################################################
fn schedule(debug_id: usize, stealers: Arc<Vec<SharedP>>, local: Worker<GPtr>) {
  const LOG_VALUE: &str = "RR_SCHED";

  let mut sched_rsp: usize = 0;
  let peers: Vec<Stealer<GPtr>> = stealers.iter().filter(|s| s.id != debug_id).map(|s| s.stealer.clone()).collect();
  let ep_fd = unsafe { epoll_create1(0) };

  if ep_fd < 0 {
    slog_r!(error, "create_epoll_error");
  } else {
    EPOLL_FD.with(|fd| fd.set(ep_fd));
  }
  register_worker(debug_id);
  WORKER.with(|w| w.set(&local as *const Worker<GPtr>));

  loop {
    let tick = TICK_QUOTA.with(|t| {
      let mut current = t.get();

      if current > 0 {
        t.set(current - 1);
      } else {
        current = 0;
      }
      current
    });

    if let Some(task) = if 0 == tick {
      handle_event(ep_fd, &local);

      let _ = get_ready_timers(&local);
      TICK_QUOTA.with(|t| t.set(RR_TICK_COUNT));
      GLOBAL_QUEUE.steal_batch_and_pop(&local).success().or_else(|| local.pop()).or_else(|| { peers.iter().rev().find_map(|s| s.steal().success()) })
      // GLOBAL_QUEUE.steal_batch_and_pop(&local).success().or_else(|| local.pop())
    } else {
      local.pop().or_else(|| GLOBAL_QUEUE.steal_batch_and_pop(&local).success()).or_else(|| { peers.iter().rev().find_map(|s| s.steal().success()) })
    } {
      unsafe {
        let ptask = task.0;

        set_thread_busy();
        CURRENT_TASK.with(|c| c.set(ptask));
        SCHED_RSP.with(|r| r.set(&mut sched_rsp as *mut usize as usize));
        swap_stack(&mut sched_rsp, (*ptask).rsp);
        CURRENT_TASK.with(|c| c.set(ptr::null_mut()));

        // eprint!("\rsched: ID: {} resumed_successfully. TICK: {}", debug_id, tick);

        if 0 == (*ptask).rsp {
          seprint_y!("🎯 ID: {} task_dropped. TICK: {} 🏁", debug_id, tick);
          let _ = Box::from_raw(ptask);
        }
      }
    } else {
      set_thread_idle();
      let sleep = get_ready_timers(&local);

      if sleep > 0 {
        thread::park_timeout(Duration::from_millis(sleep)); //  %CPU  %MEM     TIME+ COMMAND
        //                                                  //   1.0   0.1   0:00.76 zero
        seprint_g!("🔥 ID: {} TASK: unparked. TICK: {} 💥", debug_id, tick);
      } else if local.is_empty() {
        seprint_r!("🔥 ID: {} parked. TICK: {} 🏁", debug_id, tick);
        thread::park();
      }
      set_thread_busy();
      seprint_g!("🔥 ID: {} unparked. TICK: {} 💥", debug_id, tick);
    }
  }
}

/*
Reactor:
    fd -> wait_queues
Scheduler:
    runnable_tasks
epoll:
    readiness_events
reactor:
    readiness_events -> runnable_tasks
*/

// ################################################################################################################################################################
