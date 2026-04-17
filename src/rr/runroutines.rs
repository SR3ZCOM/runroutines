use std::{thread, time::{Instant, Duration}, sync::{Arc, Once}, collections::BinaryHeap, cmp::Ordering};//, atomic::{AtomicUsize, Ordering}}};
use crossbeam_deque::{Injector, Worker, Stealer/*, Steal*/};
use crossbeam_queue::SegQueue;

type TaskFn = unsafe extern "C" fn(*mut ());

const STACK_SIZE: usize = 1024 * 1024;
const RR_THREADS_COUNT: usize = 4;
const RR_TICK_COUNT: u32 = 61;

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

impl PartialEq for TimerTask {
  fn eq(&self, other: &Self) -> bool { self.when == other.when }
}
impl Eq for TimerTask {}

impl Ord for TimerTask {
  fn cmp(&self, other: &Self) -> Ordering { other.when.cmp(&self.when) }
}

impl PartialOrd for TimerTask {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
static RUNTIME_INIT: Once = Once::new();
static GLOBAL_QUEUE: once_cell::sync::Lazy<Injector<GPtr>> = once_cell::sync::Lazy::new(Injector::new);
static HANDLES: once_cell::sync::Lazy<SegQueue<thread::Thread>> = once_cell::sync::Lazy::new(SegQueue::new);
static TIMERS: once_cell::sync::Lazy<std::sync::Mutex<BinaryHeap<TimerTask>>> = once_cell::sync::Lazy::new(|| std::sync::Mutex::new(BinaryHeap::new()));

thread_local! {
  static SCHED_RSP: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
  static CURRENT_TASK: std::cell::Cell<*mut RunroutineStruct> = const { std::cell::Cell::new(std::ptr::null_mut()) };
  static LOCAL_WORKER: std::cell::Cell<*const Worker<GPtr>> = const { std::cell::Cell::new(std::ptr::null()) };
  static TICK_COUNT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

macro_rules! rr_println {
  ($($arg:tt)*) => {{
    println!($($arg)*);
    back_yield();
  }};
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
      let runroutine = Box::into_raw(Box::new(RunroutineStruct {
        rsp: sp as usize,
        stack,
      }));
      GLOBAL_QUEUE.push(GPtr(runroutine));
      eprintln!("ADD: pushed_to_global: {}", GLOBAL_QUEUE.len());

      if let Some(t) = HANDLES.pop() {
        t.unpark();
        HANDLES.push(t);
      }
    }
  }
}

// ################################################################################################################################################################
unsafe extern "C" { fn swap_stack(old_sp: *mut usize, new_sp: usize); }

// ################################################################################################################################################################
#[unsafe(no_mangle)]
unsafe extern "C" fn shim(func: TaskFn, data: *mut ()) {
  eprintln!("shim_started");
  unsafe { func(data); }
  eprintln!("shim_completed");
}

// ################################################################################################################################################################
/// # Safety
///
#[unsafe(no_mangle)]
unsafe extern "C" fn ret_to_sched() {
  unsafe {
    let ptask = CURRENT_TASK.with(|c| c.get());
    let psched_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if ptask.is_null() || psched_rsp.is_null() { return; }

    let sched_rsp = *psched_rsp;
    let mut discard_stack: usize = 0;

    (*ptask).rsp = 0;
    swap_stack(&mut discard_stack, sched_rsp);
  }
}

// ################################################################################################################################################################
pub fn sleep_yield(ms: u64) {
  let ptask = CURRENT_TASK.with(|c| c.get());

  if ptask.is_null() { return; }

  let wake_at = Instant::now() + Duration::from_millis(ms);
  let mut lock = match TIMERS.lock() {
    Ok(guard) => guard,
    Err(poisoned) => poisoned.into_inner(),
  };

  lock.push(TimerTask {
    when: wake_at,
    task: GPtr(ptask),
  });
  drop(lock);

  unsafe {
    let pshed_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if pshed_rsp.is_null() { return; }

    let sched_rsp = *pshed_rsp;
    swap_stack(&mut (*ptask).rsp, sched_rsp);
  }
}

// ################################################################################################################################################################
pub fn back_yield() {
  let ptask = CURRENT_TASK.with(|c| c.get());
  if ptask.is_null() { return; }

  unsafe {
    LOCAL_WORKER.with(|w| {
      let ptr = w.get();

      if ! ptr.is_null() {
        eprintln!("yield: pushed_to_local_before: {}", (*ptr).len());
        (*ptr).push(GPtr(ptask));
        eprintln!("yield: pushed_to_local: {}", (*ptr).len());
      } else {
        GLOBAL_QUEUE.push(GPtr(ptask));
        eprintln!("yield: pushed_to_global");
      }
    });
    let pshed_rsp = SCHED_RSP.with(|r| r.get()) as *mut usize;

    if pshed_rsp.is_null() { return; }

    let sched_rsp = *pshed_rsp;
    swap_stack(&mut (*ptask).rsp, sched_rsp);
  }
}

// ################################################################################################################################################################
fn scheduler_loop(id: usize, stealers: Arc<Vec<SharedP>>, local: Worker<GPtr>) {
  let mut sched_rsp: usize = 0;
  let peers: Vec<Stealer<GPtr>> = stealers.iter().filter(|s| s.id != id).map(|s| s.stealer.clone()).collect();

  HANDLES.push(thread::current());
  LOCAL_WORKER.with(|w| w.set(&local as *const Worker<GPtr>));

  loop {
    let tick = TICK_COUNT.with(|t| {
      let next = t.get().wrapping_add(1);
      t.set(next);
      next
    });
    let now = Instant::now();

    if let Ok(mut timers) = TIMERS.lock() {
      while let Some(t) = timers.peek() {
        if t.when > now {
          break;
        }

        if let Some(t_task) = timers.pop() {
          GLOBAL_QUEUE.push(t_task.task);

          if let Some(h) = HANDLES.pop() {
            h.unpark();
            HANDLES.push(h);
          }
        }
      }
    }

    if let Some(task) = if tick.is_multiple_of(RR_TICK_COUNT) {
      GLOBAL_QUEUE.steal_batch_and_pop(&local).success().or_else(|| local.pop()).or_else(|| { peers.iter().find_map(|s| s.steal().success()) })
    } else {
      local.pop().or_else(|| GLOBAL_QUEUE.steal_batch_and_pop(&local).success())
           .or_else(|| { peers.iter().find_map(|s| s.steal().success()) }) } {
      unsafe {
        let ptask = task.0;
        CURRENT_TASK.with(|c| c.set(ptask));
        SCHED_RSP.with(|r| r.set(&mut sched_rsp as *mut usize as usize));
        swap_stack(&mut sched_rsp, (*ptask).rsp);
        CURRENT_TASK.with(|c| c.set(std::ptr::null_mut()));

        eprintln!("sched: {} resumed_successfully. TICK: {}", id, tick);

        if 0 == (*ptask).rsp {
          eprintln!("sched: {} task_dropped. TICK: {}", id, tick);
          let _ = Box::from_raw(ptask);
        }
      }
    } else {
      // thread::park_timeout(Duration::from_millis(10)); //  %CPU  %MEM     TIME+ COMMAND
                                                       //   1.0   0.1   0:00.76 zero
      thread::park();
    }
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

    for i in 0..n {
      let local = workers.remove(0);
      let s_clone = Arc::clone(&shared_stealers);

      thread::spawn(move || {
        scheduler_loop(i, s_clone, local);
      });
    }
    eprintln!("🚀 RunRoutines runtime started with {} worker threads", n);
  });
}

// ################################################################################################################################################################
