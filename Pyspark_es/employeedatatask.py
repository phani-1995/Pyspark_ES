from methods import read_data
df = read_data()
print(df.show())

def emp_cnt():
    try:
        # Showing the different regions
        df.select("Region").distinct().show()
        # number of employees based on Region
        df.groupBy('Region').count().show()
        # Number of employees based on Country
        df.groupBy('County').count().show()
        # Number of employees based on city
        df.groupBy('City').count().show()
    except SyntaxError:
        print("Records not found")
        pass


# orderby gender and salary
def gen_sal():
    gender = df.orderBy(df.Gender)
    gender.show()
    print("Total no of male employees")
    print(gender.filter(gender.Gender == 'M').count())
    print("Total no of Female employees")
    print(gender.filter(gender.Gender == 'F').count())


def emp_sal_data():
    # Salary wise sort employee data
    df.sort("Salary").select('Emp ID', 'Salary', 'County').show()
    print("Average salary of a employee is ")
    df.agg({'Salary': 'avg'}).show()
    print("Maximum salary of a employee is ")
    df.agg({'Salary': 'max'}).show()
    print("Minimum salary of a employee is ")
    df.agg({'Salary': 'min'}).show()


# Employee Summary table
def emp_summary():
    summary = df.select('Emp ID', 'Salary', 'Region')
    summary.show()
    # printing the summary
    summary.describe().show()

    print("Total employees whose salary is greater than average salary")
    summary.filter(summary['Salary'] > 120288).count()

    print("Total employees whose salary is below average salary")
    summary.filter(summary['Salary'] < 120288).count()

    # Employess from different regions
    Midwest = summary.filter(summary['Region'] == 'Midwest').count()
    print("Total Employees from Midwest: ", Midwest)

    South = summary.filter(summary['Region'] == 'South').count()
    print("Total Employees from South: ", South)

    West = summary.filter(summary['Region'] == 'West').count()
    print("Total Employees from West: ", West)

    Northeast = summary.filter(summary['Region'] == 'Northeast').count()
    print("Total Employees from Northeast: ", Northeast)



if __name__ == "__main__":
    emp_cnt()
    gen_sal()
    emp_sal_data()
    emp_summary()




