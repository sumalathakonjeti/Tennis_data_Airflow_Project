# Tennis_data_Airflow_Project

<img width="1419" alt="Screen Shot 2021-01-20 at 4 14 24 PM" src="https://user-images.githubusercontent.com/72820961/105239684-e13c7a80-5b3a-11eb-9d61-3c41576273fc.png">


## Project Description:
Utilized various technologies, such as Airflow,  DAGs,Pandas,  Jupyter Notebook, Papermill to build  an  end  to  end  datapipeline for extracting data and then processing the data in the pipeline by using Jupyter notebook.

https://public.tableau.com/profile/sumalatha.konjeti#!/vizhome/TopTennisPlayers/Dashboard1?publish=yes

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
