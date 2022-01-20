# Performance evaluation of Terapixel rendering in Cloud (Super) Computing

## 1. Introduction
Terapixel images are images composed of 1 trillion pixels and can be used to
present information to stakeholders intuitively, however, it requires a lot of computing
power. So, it is advantageous to analyze the data collected from cloud
supercomputing infrastructure to optimize the process and reduce the cost incurred.
The challenge that was addressed in this problem was, how massive scale
supercomputer resources will be delivered for the computation of terapixel
visualization of Newcastle Upon Tyne.
Our task here is to analyze the data collected from the application checkpoint and
system metric output from the production of a terapixel image and improve the
cost-efficiency. 

## 2. Objective
This analysis is done for the user who wants to process the terapixel image on cloud platform and has below mentioned requirements.

2.1. Reduce the cost.

> Using cloud resources for heavy computation requirements is definitely cheaper as one need not invest huge upfornt moeney into hardware and can pay as per usage. However, this amount can still go really high when resources are not manages peoperly.

2.2. Improve the performance of rendering of image.

> There are lots of factors that can affect the perfromance such as, how much power is drawn is drawn by a particular GPU, when does the GPU starts to throttle, how much GPU is being utilized, memory consumption of the GPU and so on. All these things will be examined in this analysis. 

## 3. Steps to execute this Project.
NOTE: Running the whole project might take extended period of time. So, don't worry you haven't done anything wrong. I had to wait too :(
1. Download/fork/clone the project by clicking [here](https://github.com/roshan-pandey/Terascope) and place all the data files in ./data/ directory or unzip the project folder provided via NESS and place all the csv files in ./data/ directory.
2. Go to project folder and Use requirements.txt file to install the packages required to run this project. Run this command "pip install -r .\requirements.txt"
3. Go to ./src/munge and run the data_manipulation.py file. This will perform all the data manipulation required and save the newly created files in ./data/ directory.
4. Go to ./notebooks/ directory and run the CSC8634_Roshan_Pandey_210113925.ipynb to generate all the plots again or you can simply see the analysis performed with the results.

## 4. Reports
There are three pdf report files:
1. EDA_Notebook_pdf_version.pdf: This file is generated via jypyter notebook, it contains all the analysis.
2. CSC8634_Roshan_Pandey_210113925.pdf: This report was created in MS Word, it contains all the work done and results from this analysis.
3. CSC8634_Roshan_Pandey_Abstract_Report.pdf: This file contains the structured abstract (Context, Objective, Method, Results, Novelty).
4. Git log file can be found in the root directory of the project. ./CSC8634_210113925_gitLog.txt

## 5. Important Consideration
This project runs perfectly fine on windows machine by following the steps mentioned in section 3. If want to run on different OS, encoding might needs to be changed for text processing.
