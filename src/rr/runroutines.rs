use crossbeam_deque::{Worker, Stealer, Steal};
use crossbeam_queue::SegQueue;

use std::{thread, ptr, cell::Cell, arch::{naked_asm, asm}, sync::{Arc, OnceLock, atomic::{AtomicBool, Ordering}}, hint};

static PREEMPT: AtomicBool = AtomicBool::new(false);

const STACK_SIZE: usize = 1024 * 1024;
const RR_THREADS_COUNT: usize = 4;

#[repr(C)]
#[derive(Debug)]
struct Context {
  rsp: *mut u8,
}

#[allow(unused)]
#[derive(Debug)]
pub struct RunroutineStruct {
  stack: Vec<u8>,
  ctx: Context,
  entry: Option<extern "C" fn()>,
  finished: bool,
}

impl RunroutineStruct {
  pub fn new(entry: extern "C" fn()) -> GPtr {
    let mut stack = vec![0u8; STACK_SIZE];
    let rsp = unsafe { init_stack(&mut stack, entry) };
    let top = unsafe { stack.as_mut_ptr().add(STACK_SIZE) };

    println!("rsp={:?}, top={:?}, diff={}", rsp, top, unsafe { top.offset_from(rsp) });

    let g = Box::new(RunroutineStruct {
      stack,
      ctx: Context { rsp: rsp as *mut u8 },
      finished: false,
      entry: Some(entry),
    });
    println!("NEW_G: rsp={:?}", g.ctx.rsp);
    assert!((g.ctx.rsp as usize) % 16 == 0);

    GPtr(Box::into_raw(g))
  }
}

#[derive(Copy, Clone)]
pub struct GPtr(*mut RunroutineStruct);

unsafe impl Send for GPtr {}
unsafe impl Sync for GPtr {}

impl GPtr {
  #[inline]
  fn as_ptr(self) -> *mut RunroutineStruct {
    self.0
  }

  #[allow(unused)]
  #[inline]
  unsafe fn as_mut<'a>(self) -> &'a mut RunroutineStruct {
    unsafe { &mut *self.0 }
  }
}

#[allow(unused)]
#[derive(Debug)]
pub struct Scheduler {
  global: SegQueue<GPtr>,
  stealers: Vec<SharedP>,
}

impl Scheduler {
  pub fn run(&self, g: GPtr) {
    self.global.push(g);
  }
}
static SCHED: OnceLock<Arc<Scheduler>> = OnceLock::new();

pub fn rr_sched() -> &'static Arc<Scheduler> {
  SCHED.get().expect("runtime not initialized")
}

struct MState {
  p: P,
  sched: Arc<Scheduler>,
  m_rsp: *mut u8,
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

thread_local! {
  static CURRENT_G: Cell<GPtr> = Cell::new(GPtr(ptr::null_mut()));
}
thread_local! {
  static M_CTX: Cell<*mut MState> = Cell::new(ptr::null_mut());
}

unsafe fn init_stack(stack: &mut [u8], entry: extern "C" fn()) -> *mut u8 {
  unsafe {
    let top = stack.as_mut_ptr().add(stack.len());
    let mut sp = (top as usize & !0xF) as *mut usize;

    sp = sp.offset(-1);
    *sp = entry as usize;

    sp = sp.offset(-1);
    *sp = trampoline as *const () as usize;

    sp as *mut u8
  }
}

fn pick_next_coroutine(p: &mut P, sched: &Scheduler) -> Option<GPtr> {
  if let Some(g) = p.runq.pop() {
    return Some(g);
  }

  if let Some(g) = sched.global.pop() {
    return Some(g);
  }

  for sp in &sched.stealers {
    if let Steal::Success(g) = sp.stealer.steal() {
      return Some(g);
    }
  }
  None
}

unsafe fn schedule_switch_back() -> ! {
  let current = CURRENT_G.with(|c| c.get());
  let state = M_CTX.with(|c| c.get());
  eprintln!{"schedule_switch_back"};

  unsafe {
    let id = (*state).p.id;
    match pick_next_coroutine(&mut (*state).p, &(*state).sched) {
      None => { eprintln!{"idle"};
        switch_context( &mut (*current.as_ptr()).ctx.rsp, &mut (*state).m_rsp );
      },
      Some(next) => {
        println!("M{} RUN_JOB", id);
        CURRENT_G.with(|c| c.set(next));
        switch_context( &mut (*current.as_ptr()).ctx.rsp, &mut (*next.as_ptr()).ctx.rsp, );
      },
    }
    hint::unreachable_unchecked();
  }
}

extern "C" fn trampoline() {
  let g = CURRENT_G.with(|c| c.get());
  unsafe {
    let entry = (*g.as_ptr()).entry.unwrap();
    entry();
    (*g.as_ptr()).finished = true;
    schedule_switch_back();
  }
}

#[unsafe(naked)]
unsafe extern "C" fn switch_context(old: *mut *mut u8, new: *mut *mut u8) {
  naked_asm!(
    "mov [rdi], rsp",
    "mov rsp, [rsi]",
    "ret",
  );
}

fn run_m(p: P, sched: Arc<Scheduler>) {
  println!("M{} started", p.id);
  CURRENT_G.with(|c| { c.set(GPtr(ptr::null_mut()));});

  let id = p.id;
  let sched = sched.clone();
  let mut state = MState { p: p, sched, m_rsp: ptr::null_mut() };

  unsafe { asm!("mov {}, rsp", out(reg) state.m_rsp); }

  M_CTX.with(|c| c.set(&mut state as *mut _));

  loop {
    if PREEMPT.swap(false, Ordering::Relaxed) {
    }

    if let Some(next) = pick_next_coroutine(&mut state.p, &state.sched) {
      println!("M{} RUN_JOB", id);
      CURRENT_G.with(|c| c.set(next));
      unsafe { switch_context( &mut state.m_rsp, &mut (*next.as_ptr()).ctx.rsp, );}
    }
    thread::park();
  }
}

fn start_runtime(sched: Arc<Scheduler>, ps: Vec<P>) {
  for p in ps {
    let s = sched.clone();
    thread::spawn(move || run_m(p, s));
  }
}

pub fn build_runtime(mut n: usize) -> Arc<Scheduler> {
  match SCHED.get() {
    Some(sched) => return sched.clone(),
    None => {},
  }

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
  SCHED.set(sched.clone()).expect("runtime already initialized");

  start_runtime(sched.clone(), ps);

  sched
}
