echo -e "AIRFLOW_UID=$(id -u)" > .env (# or maybe I don't even need this...)
docker-compose up airflow-init
docker-compose up

http://localhost:8080
airflow/airflow.


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

**Task 2:**
Run your DAG, then ./day-2, then your DAG again.

Outcome 1: The dashboard still looks the same. But you are now able to investigate the problem by viewing the data version from yesterday.

CODE

Insight: The immutable staged user data allows you to reproduce any state of data. It reveals, in this case, that the dashboard isn't showing 
"order volume by status" but rather "order volume attributed to current status". What probably is more appropriate is "order volume attributed
to status at the time of order".

## Day 3: orders!##
Run ./day-passes again to get to day 3. 

Look at the dashboard again. The result looks odd! How you'd wish you could just change the data back to yesterdays version right?

Let's time travel and build this ability.

**Task 1: Aggregate**

 - (./day-2)
 - put orders into imported_orders/{DATE}/orders.csv
 - Create an aggregated sales model, keep the import date.


**Task 2: Roll Back**
 - Create a "view" to see all orders till date X by using the "current file" (store currently deployed data version).
 - I've taken the liberty to already put a "./roll-back script"

 **Play it again**
 - ..
 - ..
 - ..

