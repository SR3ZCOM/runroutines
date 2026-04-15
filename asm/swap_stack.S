.intel_syntax noprefix
.text
.global swap_stack

swap_stack:
  # rdi = *mut usize (old_sp), rsi = usize (new_sp)
  push rbp
  push rbx
  push r12
  push r13
  push r14
  push r15

  mov [rdi], rsp
  mov rax, rsi

  mov rdi, [rax + 64]
  mov rsi, [rax + 72]

  mov rsp, rax

  pop r15
  pop r14
  pop r13
  pop r12
  pop rbx
  pop rbp

  ret
