# Tennis_data_Airflow_Project

<img width="1193" alt="Tennis_airflow" src="https://user-images.githubusercontent.com/72820961/104627741-6a4c4100-5665-11eb-96c6-0fb8ee024301.png">


## Project Description:
Utilized various technologies, such as Airflow,  DAGs,Pandas,  Jupyter Notebook, Papermill to build  an  end  to  end  datapipeline for extracting data and then processing the data in the pipeline by using Jupyter notebook

## Use Case:
I took ATP tennis data for this project and analyzed who are the best players on different fields such as USOpen, Wimbledon, French Open and Australian Open

## How it works:
• Used the api for downloading the required APT Tennis data.The data consists 3 csv files.                                                                         
• Cleanse the data using pandas to delete unwanted data and create a csv file of cleansed data for further processing                                      
• Merge the three csv files into one merge file                                                                                                                   
• Use papermill to call the jupyter notebooks to aggregate the data for each type - Wimbledon, French Open, Australian Open and US Open  

## Tools Used:
 • Airflow
 • Pandas
 • Papermill
 • Jupyter notebook
