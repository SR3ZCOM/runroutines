use std::{thread};
use runroutines::{rr_println, rr::runroutines::{build_runtime, arbit_yield, RunroutineStruct, /*CYAN, */GREEN, NC}};
use rrmacro::rr_compliant;

struct TaskStruct{
  msg: String,
  id: i32,
}

// ################################################################################################################################################################
extern "C" fn task1(pdata: *mut ()) {
  let msg = unsafe { &*(pdata as *const String) };

  for i in 1..10 {
    rr_println!("⚠️  MAIN: {} TASK_1: {}", i, msg);
  }
}

// ################################################################################################################################################################
#[rr_compliant]
extern "C" fn task2(pdata: *mut ()) {
  unsafe {
    let task_st = Box::from_raw(pdata as *mut TaskStruct);

    for i in 1..10 {
      rr_println!("⚠️  MAIN: {} TASK_2: {}: {}", i, task_st.msg, task_st.id);
    }
  }
}

// ################################################################################################################################################################
#[test]
fn rr_run() {
  log::info!("✅ RR_TEST");

  RunroutineStruct::add(task1, String::from("new_msg"));
  let task_st = TaskStruct { msg: String::from("new_msg_2"), id: 777};

  RunroutineStruct::add(task2, task_st);
  build_runtime(2);

  thread::sleep(std::time::Duration::from_millis(1));
  println!("🎯 {}RR_TEST_COMPLETED{} 🏁", GREEN, NC);
}

// ################################################################################################################################################################
