# -*- coding: utf-8 -*-
"""
Created on Sat Oct 19 11:52:53 2019

@author: Lenovo
"""

import pandas

data = pandas.read_csv("../Original Data/earthquake_data.csv")

data = data[['What is your gender?','Age','Do you think the "Big One" will occur in your lifetime?']]

data = data.rename(columns={'What is your gender?':'Gender', 'Do you think the "Big One" will occur in your lifetime?':'Will the Big One occur in your lifetime?'})
data = data.groupby(['Gender','Age'])['Will the Big One occur in your lifetime?'].apply(lambda x: x.value_counts()).reset_index()
data.columns = ["Gender", "Age", "Will the Big One occur in your lifetime?", "Number of people who answered so"]
print(data)
data.to_csv(r"../Analysis Data/results.csv")
