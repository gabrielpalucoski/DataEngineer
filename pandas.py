import pandas as pd

# Load a CSV file into a Pandas DataFrame
df = pd.read_csv("data.csv")

# Print the first 5 rows of the DataFrame
print(df.head())

# Calculate the mean of a column
mean = df["column_name"].mean()
print("Mean:", mean)

# Filter rows based on a condition
filtered_df = df[df["column_name"] > mean]
print("Filtered DataFrame:")
print(filtered_df)
