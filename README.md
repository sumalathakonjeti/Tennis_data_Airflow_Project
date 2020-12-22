# Tennis_data_Airflow_Project
Utilized various technologies, such as Airflow,  DAGs,Pandas,  Jupyter Notebook,  to build  an  end  to  end  datapipeline for extracting data and then processing the data in the pipeline by using Jupyter notebook

## Use Case:
I took ATP tennis data for this project and analyzed who are the best players on different fields such as USOpen, Wimbledon, French Open and Australian Open

## How it works:
• Used the api for downloading the required APT Tennis data.The data consists 3 csv files.                                                                         
• Cleanse the data using pandas to delete unwanted data and create a csv file of cleansed data for further processing                                      
• Merge the three csv files into one merge file                                                                                                                   
• Use papermill to call the jupyter notebooks to aggregate the data for each type - Wimbledon, French Open, Australian Open and US Open  
