use crossbeam_deque::{Injector, Worker, Stealer, Steal};
use crossbeam_queue::SegQueue;

use std::{thread, sync::{Arc, Once}};

type TaskFn = unsafe extern "C" fn(*mut ());

const STACK_SIZE: usize = 1024 * 1024;
const RR_THREADS_COUNT: usize = 4;

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
pub struct RunroutineStruct {
  rsp: usize,
  stack: Vec<u8>,
}

#[derive(Copy, Clone)]
struct GPtr(*mut RunroutineStruct);

unsafe impl Send for GPtr {}
unsafe impl Sync for GPtr {}

#[allow(unused)]
#[derive(Debug)]
struct Scheduler {
  global: SegQueue<GPtr>,
  stealers: Vec<SharedP>,
}

#[allow(unused)]
struct P {
  id: usize,
  runq: Worker<GPtr>,
}

#[allow(unused)]
#[derive(Debug)]
struct SharedP {
  id: usize,
  stealer: Stealer<GPtr>,
}

static RUNTIME_INIT: Once = Once::new();
static GLOBAL_QUEUE: once_cell::sync::Lazy<Injector<GPtr>> = once_cell::sync::Lazy::new(Injector::new);

thread_local! {
  static SCHED_CTX: std::cell::UnsafeCell<Context> = std::cell::UnsafeCell::new(Context::default());
  static CURRENT: std::cell::Cell<*mut Context> = const { std::cell::Cell::new(std::ptr::null_mut()) };
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
    }
  }
}

thread_local! { static LOCAL: Worker<GPtr> = Worker::new_fifo(); }

// ################################################################################################################################################################
fn fetch_task(local: &Worker<GPtr>) -> Option<GPtr> {
  if let Some(g) = local.pop() {
    return Some(g);
  }

  if let Steal::Success(g) = GLOBAL_QUEUE.steal() {
    return Some(g);
  }
  None
}

// ################################################################################################################################################################
unsafe extern "C" { fn swap_stack(old_sp: *mut usize, new_sp: usize); }

// ################################################################################################################################################################
#[unsafe(no_mangle)]
unsafe extern "C" fn shim(func: TaskFn, data: *mut ()) {
  println!("shim_started");
  unsafe { func(data); }
  println!("shim_completed");
}

thread_local! {
  static SCHED_RSP: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// ################################################################################################################################################################
/// # Safety
///
#[unsafe(no_mangle)]
unsafe extern "C" fn ret_to_sched() {
  unsafe {
    let s_rsp_ptr = SCHED_RSP.with(|r| r.get()) as *mut usize;
    if s_rsp_ptr.is_null() { return; }

    let s_rsp = *s_rsp_ptr;
    let mut dummy = 0;
    swap_stack(&mut dummy, s_rsp);
  }
}

// ################################################################################################################################################################
fn scheduler_loop() {
  let mut sched_rsp: usize = 0;

  loop {
    let task = LOCAL.with(fetch_task);

    if let Some(g) = task {
      unsafe {
        let ptask = g.0;
        SCHED_RSP.with(|r| r.set(&mut sched_rsp as *mut usize as usize));
        swap_stack(&mut sched_rsp, (*ptask).rsp);
        println!("SCHED RESUMED SUCCESSFULLY");
        let _ = Box::from_raw(ptask);
      }
    } else {
      std::hint::spin_loop();
    }
  }
}

// ################################################################################################################################################################
pub fn build_runtime(mut n: usize) {
  RUNTIME_INIT.call_once(|| {
    if 0 == n {
      n = thread::available_parallelism().map(|n| n.get()).unwrap_or(RR_THREADS_COUNT);
    }
    let mut stealers = Vec::new();
    let mut ps = Vec::new();

    for i in 0..n {
      let worker = Worker::new_fifo();

      stealers.push(SharedP { id: i, stealer: worker.stealer() });
      ps.push(P { id: i, runq: worker });
    }
    let sched = Arc::new(Scheduler {
      global: SegQueue::new(),
      stealers,
    });

    for _ in 0..n {
      let _ = Arc::clone(&sched);
      std::thread::spawn(move || {
        scheduler_loop();
      });
    }
    println!("🚀 RunRoutines runtime started with {} worker threads", n);
  });
}

// ################################################################################################################################################################
