from es import *

try:
    regi = region()
    print(regi)
    print("-----------------------------------------------------------------------------------------")
    cnt_emp = cnt_emp_reg()
    print(cnt_emp)
    print("-----------------------------------------------------------------------------------------")

    sal = gen_sal()
    print(sal)
    print("-----------------------------------------------------------------------------------------")

    srt = srt_emp()
    print(srt)
    print("-----------------------------------------------------------------------------------------")

    cnt_mnt = cnt_emp_mnt()
    print(cnt_emp)
    print("------------------------------------------------------------------------------------------")

    jan = exp_quries()
    print("Dispalying the employees who belong to midwest and joined in jan month")
    print(jan)
except:
    print("Unknown method")

