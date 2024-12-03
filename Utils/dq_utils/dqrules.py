##### DQ Rules #####

######### Comparision Rules ###############

# Checks if a sensor value is equal to the expected value. ONLY ALLOWS INTEGER VALUES FOR expeceted_value.
def equals_integer(sensor_value, expected_value):
    if isinstance(expected_value, int):
        passed = (sensor_value == expected_value)
        return passed
    else:
        return TypeError

# Checks if a sensor value is equal to the expected value. ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def equals(sensor_value, expected_value):
    if isinstance(expected_value, int) | isinstance(expected_value, float):
        passed = (sensor_value == expected_value)
        return passed
    else:
        return TypeError
    
# Checks if a sensor value is equal to the expected value. ONLY ALLOWS STRING VALUES FOR expeceted_value.
def equals_string(sensor_value, expected_value):
    if isinstance(expected_value, str):
        passed = (sensor_value == expected_value)
        return passed
    else:
        return TypeError
    

# Checks if a sensor value is within the error margin given on the expected value. ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value and error margin
def equals_with_error_margin(sensor_value, expected_value, error_margin):
    if (isinstance(expected_value, int) | isinstance(expected_value, float)) & (isinstance(error_margin, int) | isinstance(error_margin, float)):
        upperbound = expected_value + error_margin
        lowerbound = expected_value - error_margin
        passed = ((sensor_value >= lowerbound) & (sensor_value <= upperbound))
        return passed
    else:
        return TypeError


# Checks if the sensor value is greater than or equal to the expected value. ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def min_with_equals(sensor_value, expected_value):
    if isinstance(expected_value, int) | isinstance(expected_value, float):
        passed = (sensor_value >= expected_value)
        return passed
    else:
        return TypeError

# Checks if the sensor value is less than or equal to the expected value. ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def max_with_equals(sensor_value, expected_value):
    if isinstance(expected_value, int) | isinstance(expected_value, float):
        passed = (sensor_value <= expected_value)
        return passed
    else:
        return TypeError

# Checks if the sensor value is between the lowerbound(inclusive) and upperbound(inclusive). ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def between_with_equals(sensor_value, lowerbound, upperbound):
    if (isinstance(lowerbound, int) | isinstance(lowerbound, float)) & (isinstance(upperbound, int) | isinstance(upperbound, float)):
        passed = ((sensor_value >= lowerbound) & (sensor_value <= upperbound))
        return passed
    else:
        return TypeError
    
# Checks if the sensor value is greater than the expected value. ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def min_without_equals(sensor_value, expected_value):
    if isinstance(expected_value, int) | isinstance(expected_value, float):
        passed = (sensor_value > expected_value)
        return passed
    else:
        return TypeError
  
# Checks if the sensor value is less than the expected value. ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def max_without_equals(sensor_value, expected_value):
    if isinstance(expected_value, int) | isinstance(expected_value, float):
        passed = (sensor_value < expected_value)
        return passed
    else:
        return TypeError


# Checks if the sensor value is between the lowerbound(exclusive) and upperbound(exclusive). ONLY ALLOWS INTEGER AND FLOAT VALUES FOR expeceted_value.
def between_without_equals(sensor_value, lowerbound, upperbound):
    if (isinstance(lowerbound, int) | isinstance(lowerbound, float)) & (isinstance(upperbound, int) | isinstance(upperbound, float)):
        passed = ((sensor_value > lowerbound) & (sensor_value < upperbound))
        return passed
    else:
        return TypeError


# Checks if the sensor value is greater than or equal to the expected value. ONLY ALLOWS INTEGER VALUES FOR expeceted_value.
def min_count_with_equals(sensor_value, expected_value):
    if isinstance(expected_value, int):
        passed = (sensor_value >= expected_value)
        return passed
    else:
        return TypeError
    
# Checks if the sensor value is less than or equal to the expected value. ONLY ALLOWS INTEGER VALUES FOR expeceted_value.
def max_count_with_equals(sensor_value, expected_value):
    if isinstance(expected_value, int):
        passed = (sensor_value <= expected_value)
        return passed
    else:
        return TypeError
    

# Checks if the sensor value is greater than the expected value. ONLY ALLOWS INTEGER VALUES FOR expeceted_value.
def min_count_without_equals(sensor_value, expected_value):
    if isinstance(expected_value, int):
        passed = (sensor_value > expected_value)
        return passed
    else:
        return TypeError


# Checks if the sensor value is less than the expected value. ONLY ALLOWS INTEGER VALUES FOR expeceted_value.
def max_count_without_equals(sensor_value, expected_value):
    if isinstance(expected_value, int):
        passed = (sensor_value < expected_value)
        return passed
    else:
        return TypeError



