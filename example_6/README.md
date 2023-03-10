
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


###

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


### Day1 : Base Setup###

- We got one chart showing order volume/day, segmented by "status".

**Task 1:**
- Use airflow (trigger the workflow) to see that it imports and works.

## Day 2:##
**Task 2:**
- Now please run ./day-passes to let "a day pass", then run our workflow again, and take a look at the chart.

You now should be concerned! At least the user of the graph would. Something seems broken...

[PIC compare yesterday to today..]

What happened? We don't know! How would we know? We don't have the state of the data yesterday.

Let's change this...

### Making users "immutable"
We're going to turn back the time to yesterday (./day-1) and make sure,
our user data is "immutable",that is unchangeable, by never overwriting over copy of it, but always storing a new one.

**Task 1:**
We're using daily imports here, so use the date as identifier for the different data versions by storing a new copy of the user data in
/imported_data/users/{DATE}/users.csv

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

