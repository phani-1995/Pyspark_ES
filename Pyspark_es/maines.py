from es import *

try:
    # Dispalying the employees who belongs to the regions midwest and south and who has hike greater than 14
    regi = region()
    print(regi)
    print("-----------------------------------------------------------------------------------------")
    # Count number of employees in each Region
    cnt_emp = cnt_emp_reg()
    print(cnt_emp)
    print("-----------------------------------------------------------------------------------------")
    # Dispalying the employess based on Gender and salary in desending order
    sal = gen_sal()
    print(sal)
    print("-----------------------------------------------------------------------------------------")
    # Sorting the Employees based on salary
    srt = srt_emp()
    print(srt)
    print("-----------------------------------------------------------------------------------------")

    # Dispalying employees joining count based on different months
    cnt_mnt = cnt_emp_mnt()
    print(cnt_emp)
    print("------------------------------------------------------------------------------------------")

    # Dispalying the employees who belong to midwest and joined in jan month
    jan = exp_quries()
    print(jan)
except:
    print("Unknown methods")

