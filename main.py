def check_age():
    age = 20
    if age >= 18 and age <= 60:
        return "Adult"
    elif age > 60:
        return "Old"
    else:
        return "Minor"
check_age()