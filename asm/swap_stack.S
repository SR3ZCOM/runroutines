.intel_syntax noprefix
.text
.global swap_stack

swap_stack:
    push rbp
    push rbx
    push r12
    push r13
    push r14
    push r15

    mov [rdi], rsp

    # New offsets for the 12-slot layout
    # 48 (regs) + 8 (shim) + 8 (ret_to_sched) = 64
    mov rdi, [rsi + 64]   # func
    mov rsi, [rsi + 72]   # data

    mov rsp, rsi

    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp

    ret
