use std::{cell, ptr, mem, thread, time::{Instant, Duration}, collections::BinaryHeap, cmp::Ordering as CmpOrdering, io::{Error as IoError}};
use std::sync::{Arc, Once, OnceLock, atomic::{AtomicU64, Ordering}};
use crossbeam_deque::{Injector, Worker, Stealer};
use libc::{epoll_wait, epoll_event, epoll_create1, epoll_ctl, /*EPOLL_CTL_DEL, */EPOLL_CTL_ADD, EPOLLIN, read, EAGAIN};

pub const NC: &str = "\x1b[0m"; // NO_COLOR
pub const DRED: &str = "\x1b[31m";
pub const RED: &str = "\x1b[91m";
pub const DGREEN: &str = "\x1b[32m";
pub const GREEN: &str = "\x1b[92m";
pub const DYELLOW: &str = "\x1b[33m";
pub const YELLOW: &str = "\x1b[93m";
pub const MAG: &str = "\x1b[95m";
pub const DCYAN: &str = "\x1b[36m";
pub const CYAN: &str = "\x1b[96m";

type TaskFn = unsafe extern "C" fn(*mut ());

const STACK_SIZE: usize = 1024 * 1024;
const RR_THREADS_COUNT: usize = 4;
const WORKERS_COUNT_MAX: usize = 64;
const RR_TICK_COUNT: u32 = 61;

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

  static TICK_COUNT: cell::Cell<u32> = const { cell::Cell::new(0) };
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
pub fn build_runtime(mut n: usize) {
  RUNTIME_INIT.call_once(|| {
    if 0 == n || n > RR_THREADS_COUNT {
      n = thread::available_parallelism().map(|n| n.get()).unwrap_or(RR_THREADS_COUNT);
    }
    let mut stealers = Vec::new();
    let mut workers = Vec::new();

    for i in 0..n {
      let worker = Worker::new_fifo();
      stealers.push(SharedP { id: i, stealer: worker.stealer() });
      workers.push(worker);
    }
    let shared_stealers = Arc::new(stealers);

    for id in 0..n {
      let local = workers.remove(0);
      let s_clone = Arc::clone(&shared_stealers);

      thread::spawn(move || {
        schedule(id, s_clone, local);
      });
    }
    log::debug!("🚀 {}RunRoutines runtime started with {} worker threads{} 🔥", GREEN, n, NC);
  });
}

// ################################################################################################################################################################
unsafe extern "C" { fn swap_stack(old_sp: *mut usize, new_sp: usize); }

// ################################################################################################################################################################
#[unsafe(no_mangle)]
unsafe extern "C" fn shim(func: TaskFn, data: *mut ()) {
  log::debug!("shim: started");
  unsafe { func(data); }
  log::debug!("shim: completed");
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
pub fn sleep_yield(ms: u64) {
  let ptask = CURRENT_TASK.with(|c| c.get());

  if ptask.is_null() { return; }

  let wake_at = Instant::now() + Duration::from_millis(ms);

  TIMERS.with(|timers| {
    timers.borrow_mut().push(TimerTask {
      when: wake_at,
      task: GPtr(ptask),
    });
  });
  log::debug!("{}sleep_yield: pushed: {:?}{}", YELLOW, wake_at, NC);

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
        // log::debug!("arbit_yield: pushed_to_local: COUNT: {}", (*ptr).len());
      } else {
        GLOBAL_QUEUE.push(GPtr(ptask));
        log::debug!("arbit_yield: pushed_to_global: COUNT: {}", GLOBAL_QUEUE.len());
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
    epoll_ctl(fd, EPOLL_CTL_ADD, fd, &mut ev);
    let psched_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if ! psched_rsp.is_null() {
      swap_stack(&mut (*ptask).rsp, *psched_rsp);
    }
  }
}

// ################################################################################################################################################################
impl RunroutineStruct {
  pub fn add<T>(func: TaskFn, data: T) {
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

      log::debug!("add: pushed_to_global: COUNT: {}", GLOBAL_QUEUE.len());

      let mask = REGISTRY.idle_mask.load(Ordering::Acquire);

      if 0 != mask {
        let id = mask.trailing_zeros() as usize;

        if id < WORKERS_COUNT_MAX {
          let old_mask = REGISTRY.idle_mask.fetch_and(!(1 << id), Ordering::SeqCst);

          if 0 != (old_mask & (1 << id)) && let Some(t) = REGISTRY.handles[id].get() {
            log::debug!("{}add: ID: {} unparked{}", GREEN, id, NC);
            t.unpark();
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

    loop {
      let expired = match timers.peek() {
        Some(t) => t.when <= now,
        None => false,
      };

      if ! expired {
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
        if next.when <= now {
          0
        } else {
          (next.when.saturating_duration_since(now).as_millis() as u64).max(1)
        }
      }
      None => 0,
    }
  })
}

// ################################################################################################################################################################
fn handle_event(epfd: i32, local: &Worker<GPtr>) {
  let mut events: [epoll_event; 64] = unsafe { mem::zeroed() };

  let n = unsafe {
    epoll_wait(epfd, events.as_mut_ptr(), 64, 0)
  };

  for event in events.iter().take(n.max(0) as usize) {
    let data = event.u64;
    let task_ptr = (data & 0xFFFF_FFFF) as usize;
    let task = GPtr(task_ptr as *mut RunroutineStruct);

    local.push(task);
  }
}

// ################################################################################################################################################################
fn schedule(debug_id: usize, stealers: Arc<Vec<SharedP>>, local: Worker<GPtr>) {
  let mut sched_rsp: usize = 0;
  let peers: Vec<Stealer<GPtr>> = stealers.iter().filter(|s| s.id != debug_id).map(|s| s.stealer.clone()).collect();
  let ep_fd = unsafe { epoll_create1(0) };

  if ep_fd < 0 {
    log::error!("❌ {}create_epoll_error{}", RED, NC);
  } else {
    EPOLL_FD.with(|fd| fd.set(ep_fd));
  }
  register_worker(debug_id);
  WORKER.with(|w| w.set(&local as *const Worker<GPtr>));

  loop {
    let tick = TICK_COUNT.with(|t| {
      let next = t.get().wrapping_add(1);
      t.set(next);
      next
    });
    let sleep = get_ready_timers(&local);

    if let Some(task) = if tick.is_multiple_of(RR_TICK_COUNT) {
      GLOBAL_QUEUE.steal_batch_and_pop(&local).success().or_else(|| local.pop()).or_else(|| { peers.iter().find_map(|s| s.steal().success()) })
    } else {
      local.pop().or_else(|| GLOBAL_QUEUE.steal_batch_and_pop(&local).success()).or_else(|| { peers.iter().find_map(|s| s.steal().success()) })
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
          log::debug!("🔥 {}sched: ID: {} task_dropped. TICK: {}{} 🏁", YELLOW, debug_id, tick, NC);
          let _ = Box::from_raw(ptask);
        }
      }

      if sleep > 0 {
        thread::park_timeout(Duration::from_millis(sleep)); //  %CPU  %MEM     TIME+ COMMAND
        //                                                  //   1.0   0.1   0:00.76 zero
        log::debug!("🔥 {}sched: ID: {} TASK: unparked. TICK: {}{}", GREEN, debug_id, tick, NC);
      }
    } else {
      handle_event(ep_fd, &local);

      if sleep > 0 {
        thread::park_timeout(Duration::from_millis(sleep)); //  %CPU  %MEM     TIME+ COMMAND
        //                                                  //   1.0   0.1   0:00.76 zero
        log::debug!("🔥 {}sched: ID: {} unparked. TICK: {}{}", GREEN, debug_id, tick, NC);
      } else {
        log::debug!("🔥 {}sched: ID: {} parked. TICK: {}{}", RED, debug_id, tick, NC);

        set_thread_idle();

        if sleep > 0 {
          thread::park_timeout(Duration::from_millis(sleep));
        } else {
          thread::park();
        }
        set_thread_busy();
      }
    }
  }
}

// ################################################################################################################################################################
