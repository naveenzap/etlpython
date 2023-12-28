import pandas as pd

# Create a DataFrame
df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie", "David", "Emily"],
    "age": [32, 28, 25, 30, 27],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
})

# Define a function to calculate the average age by city
def average_age_by_city(df):
    average_age = df.groupby('city')['age'].mean()
    return average_age

# Define a function to filter the DataFrame by city
def filter_by_city(df, city):
    filtered_df = df[df['city'] == city]
    return filtered_df

# Call the average_age_by_city function
average_age_by_city = average_age_by_city(df)
print(average_age_by_city)

# Call the filter_by_city function
filtered_df = filter_by_city(df, 'Los Angeles')
print(filtered_df)


import pandas as pd

# Create a DataFrame
df = pd.DataFrame({
    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Emily'],
    'Age': [32, 28, 25, 30, 27],
    'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
})

# Group data by city and calculate average age
average_age_by_city = df.groupby('City')['Age'].mean()
print(average_age_by_city)

# Create a bar chart to visualize average age by city
import matplotlib.pyplot as plt

average_age_by_city.plot(kind='bar')
plt.title('Average Age by City')
plt.show()
