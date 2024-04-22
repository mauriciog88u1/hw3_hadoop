import pandas as pd
import matplotlib.pyplot as plt

file_path = 'q10_data.csv'  
data = pd.read_csv(file_path, header=None, names=['Year', 'Region_Genre', 'Count'])

data['Region'] = data['Region_Genre'].apply(lambda x: x.split('_')[0].split(';')[0])

region_yearly_data = data.groupby(['Year', 'Region'])['Count'].sum().unstack(fill_value=0)

plt.figure(figsize=(14, 7))
region_yearly_data.plot(kind='line', marker='o')
plt.title('Popularity of Spanish Songs Per Region Per Year')
plt.xlabel('Year')
plt.ylabel('Total Song Counts')
plt.grid(True)
plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
