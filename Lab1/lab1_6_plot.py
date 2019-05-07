import matplotlib.pyplot as plt
import pandas as pd


diff = pd.read_csv('results_/lab1_6/part-00000.csv',
    names = ["Date", "Temperature"]);

# readings = pd.read_csv('results_/lab1_6_readings/part-00000.csv',
#     names = ["Date", "Temperature"]);


plt.plot(diff.Date, diff.Temperature, 'b', linewidth = 0.5, label='Difference')
# plt.plot(readings.Date, readings.Temperature, 'r', linewidth = 0.5, label='Readings')
plt.legend(loc='upper right')
plt.ylabel('Temperature')
plt.xlabel('Date')
plt.xticks(diff.Date[::30], rotation=45)
plt.show()
