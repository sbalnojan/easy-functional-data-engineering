
<img src="blank_lambda.png" width="400px" />

# Functional Data Engineering with Python/Airflow Tutorial #
Experience the benefits of functional data engineering first hand in a simple Airflow & Python based Setup.
Convert a regular data pipeline to a functional one in two simple steps.

## What's in it
- Super simple setup, use the supplied devcontainer in your local codespaces, open up GitHub or use the dockerized setup.
- See two typical problems solved by functional data engineering
- Experience how to handle stateful and immutable data differently.
- Get excited about the benefits of functional data engineering.

INCLUDE mini GIF of going from "what data changed? I have no idea to => let me quickly check yesterdays data, => 

## Let's get started

1. Run ```docker-compose up airflow-init``` to initialize the db.
2. Run ```docker-compose up``` to open up airflow (on start up, jupyter_1 will dump a token, copy it for the next step!)
3. Run ```./day-1``` to set the time to day 1. Open up: http://localhost:8080 (pwd and users: airflow/airflow).

## Inspect & run the DAG
1. Take a look at the dag "user_data_dag". It imports orders & users from the sources. 

<img src="dag_01.png" width="400px" />

2. Trigger a DAG run (it should succeed), and then open up Jupyter at port 8888 (you get the token in the docker-compose start)
3. Open up the work/order_status_chart notebook and take a look at the dashboard. Sales in both segments are growing slightly.

<img src="dashboard_01.png" width="400px" />

## Let's hit a problem with the user import
Everything is looking good right? Let us progress to the next day then.

1. Run ```./day-2```, then trigger the DAG again.
2. Then refresh the dashboard. You get a new data point for the line chart, but also something strange happens. Data from yesterday will change.

<img src="dashboard_02.png" width="400px" />

How is that possible? The order import just appends orders, so it cannot "change data from yesterday".

3. But take a look into the code for the user import. [users_orders.py](dags/load_data/users_orders.py):
```python
def load_users():
    user_data = pd.read_csv(F"/opt/airflow/raw_data/users_{today}.csv")
    user_data.head()
    user_data.fillna({
        'name': 'not known', 
        'status': 'standard'
    }, inplace=True)
    user_data.to_csv("/opt/airflow/imported_data/users.csv", index=False)

```
The third line does some cleaning, and the final line saves the new user state, overwriting the old state. 

**Problem:** This is common practice. The user "source" doesn't provide any kind of dates, and we do need the current state of users to do analytical work. But that means we won't be able to figure out our problem. 

**Solution:** So let us implement functional data engineering here to get reproducibility of yesterdays results while still keeping the current state for analysis.

## Making users immutable ##

1. Turn time back to yesterday by running ```./day-1```
2. Adapt the DAG [users_orders.py](dags/load_data/users_orders.py) to save user data each day to a new file. Use the path ```/imported_data/users/{DATE}/users.csv```.  Use the supplied variable ```today``` to make this happen.

```python 
    user_data = pd.read_csv(F"/opt/airflow/raw_data/users_{today}.csv")
    user_data.head()
    user_data.fillna({
        'name': 'not known', 
        'status': 'standard'
    }, inplace=True)

    #mkdir if not exist
    import os  
    os.makedirs(f"/opt/airflow/imported_data/{today}/", exist_ok=True)  

    user_data.to_csv(f"/opt/airflow/imported_data/{today}/users.csv", index=False)

```
We're creating the directory if it doesn't yet exist, and  use the ```today``` variable to get todays date, and then place todays user state into this directory.


3. Our DAG doesn't end at the import however. So you also need to add the timestamp to the next part of the dag, the task ```process_user_orders```:

```python
def process_users_orders():
    user_data = pd.read_csv(f"/opt/airflow/imported_data/{today}/users.csv")
    order_data = pd.read_csv("/opt/airflow/imported_data/orders.csv")

    result = order_data.merge(user_data, on="user_id", how="left") #

    #mkdir if not exist
    import os  
    os.makedirs(f"/opt/airflow/processed_data/{today}/", exist_ok=True)  

    result.groupby(["sales_date","status"]).sum().reset_index().to_csv(f"/opt/airflow/processed_data/{today}/agg_sales.csv")

```
We're using the same logic here. Use the date of today, create the directory if it doesn't exist yet and then store the results  into a new directory.


4. Now run our new DAG!

4. Next adapt the dashboard to use todays state.

```python 
sales = pd.read_csv(f"processed_data/{today}/agg_sales.csv", header=0)
``` 

5. Now refresh your dashboard, just to be sure that it works. 



**Task 1:**
We're using daily imports here, so use the date as identifier for the different data versions by storing a new copy of the user data in

You also need to redo the dashboard, to always read the latest folder (you can use the utility function already inside the notebook)...

You also need to change the loading to todays thing... We can recreate the final graph as well (adaptthat one as well!)

**Task 2:**
Run your DAG, then ./day-2, then your DAG again.

Outcome 1: The dashboard still looks the same. But you are now able to investigate the problem by viewing the data version from yesterday.

Outcome 2: You can now investigate inside the notebook (display today and yesterday, and then the same for the user data.).
CODE

Insight: The immutable staged user data allows you to reproduce any state of data. It reveals, in this case, that the dashboard isn't showing 
"order volume by status" but rather "order volume attributed to current status". What probably is more appropriate is "order volume attributed
to status at the time of order".

## Day 3: orders!##
Run ./day-3 again to get to day 3. 

Look at the dashboard again. The result looks odd! How you'd wish you could just change the data back to yesterdays version right?

Let's time travel and build this ability.

**Task 1: Aggregate**

 - (./day-2)
 - put orders into imported_orders/{DATE}/orders.csv
 - UPDATE THE SALES MODEL, by joining all orders together... Create an aggregated sales model, keep the import date!


**Task 2: Roll Back**
 - Create a "view" to see all orders till date X by using the "current file" (store currently deployed data version).
 - I've taken the liberty to already put a "./roll-back script"

 **Play it again**
 - ..
 - ..
 - ..






---
Plan:
1. Full import Users
2. Incremental import Orders

Do three things for FDE:
1. Handle time stamped order stuff (before just append, then partition)
2. Handle full import of users (partition)
3. Add a view on top..

For second tut:
1. Handle late arriving facts
2. Use tax example to keep logic in data.

## Functional Data Engineering Tutorial
We're going to make pasta. With sauce. We're producing two outcomes here, with a few ingredients each.
