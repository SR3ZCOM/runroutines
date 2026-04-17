use std::{thread};
use runroutines::rr::runroutines::{build_runtime, back_yield, RunroutineStruct};

extern "C" fn task1(data: *mut ()) {
  // thread::sleep(std::time::Duration::from_millis(1000));
  let msg = unsafe { &*(data as *const String) };

  for i in 1..10 {
    println!("⚠️  TASK_1: {}", msg);

    if i % 5 == 0 {
      back_yield();
    }
  }
}

extern "C" fn task2(_: *mut ()) {
  // thread::sleep(std::time::Duration::from_millis(500));
  for i in 1..10 {
    eprintln!("⚠️  TASK_2");
    if i % 5 == 0 {
      back_yield();
    }
  }
}

// ################################################################################################################################################################
#[test]
fn rr_run() {
  log::info!("✅ RR_TEST");
  build_runtime(1);

  log::info!("✅ RR_TEST: BEFORE_ADD");

  RunroutineStruct::add(task1, String::from("new_msg"));
  RunroutineStruct::add(task2, std::ptr::null_mut::<i32>());

  loop {
    thread::sleep(std::time::Duration::from_millis(1));
  }
}

// ################################################################################################################################################################
