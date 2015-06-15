from __future__ import division
from pandas import *
from pandas.tseries.offsets import *
import numpy as np
import monetdb.sql

loans = pandas.read_csv("data/LoanStats3.csv",
                        encoding='utf-8',
                        parse_dates=['issue_d','last_pymnt_d','next_pymnt_d','last_credit_pull_d'],
                        index_col=0,
                        infer_datetime_format=True,
                        low_memory=True,
                        na_values = '*')

# Pandas parses the dates as mid-month, but we need first of the month
mb = MonthBegin()
loans.issue_d = loans.issue_d.map(lambda d: d-mb)

riskFreeRates = pandas.read_csv("data/RiskFreeRates.csv",
                        encoding='utf-8',
                        parse_dates=['observation_date'],
                        index_col=0,
                        infer_datetime_format=True)


# Note: MonetDB must be installed and populated first by calling the lendingClub.R loadHistory function
# Raw data from http://additionalstatistics.lendingclub.com/ "Payments made to investors"
cursor = connection.cursor()
cursor.arraysize = 1000
sql = """
    SELECT
        loan_id,
        mob,
        case when pco_recovery_investors is not null then
            pco_recovery_investors - pco_collection_fee_investors
        else
            0
        end + received_amt_investors as received,
        due_amt_investors as due,
        pbal_beg_period_investors as balance,
        period_end_lstat
    FROM history_inv
    --WHERE loan_id in (1077501, 1077430, 1075358)
"""
cursor.execute(sql)

output = []
prev_loan_id = -1
row = cursor.fetchone() 
while row is not None:
    loan_id, mob, received, due, balance, status = row
    #print row
    if loan_id != prev_loan_id:
        # Output previous loan stats
        if prev_loan_id != -1:
            output.append((prev_loan_id, finalStatus, prev_mob, missedPayment, firstMissed, dueWhenFirstMissed, receivedAfterMissed, riskFreeRate, npv/investment))
        
        # Set up for new loan
        loan = loans.loc[loan_id]
        if loan.term == ' 36 months':
            riskFreeRate = riskFreeRates.loc[loan.issue_d].treasury3year
        else:
            riskFreeRate = riskFreeRates.loc[loan.issue_d].treasury5year
        monthlyDiscountRate = (1 + riskFreeRate/100) ** (1/12)

        investment = loan.funded_amnt_inv
        npv = -investment
        prev_mob = -1
        prev_loan_id = loan_id
        firstMissed = -1
        missedPayment = False
        receivedAfterMissed = 0
        dueWhenFirstMissed = 0
        
    # Skip duplicate rows
    if mob == prev_mob: 
        next
    prev_mob = mob
    finalStatus = status
        
    npv += received / (monthlyDiscountRate ** mob)
    totalMonths = mob
    if missedPayment:
        receivedAfterMissed += received
    if not missedPayment and due>0 and received/due < 0.99:
        firstMissed = mob
        missedPayment = True
        dueWhenFirstMissed = balance
        
    row = cursor.fetchone()

output.append((prev_loan_id, finalStatus, prev_mob, missedPayment, firstMissed, dueWhenFirstMissed, receivedAfterMissed, riskFreeRate, npv/investment))
outcomedf = DataFrame.from_records(output, index='loan_id',
    columns=['loan_id', 'finalStatus', 'monthsObserved', 'missedPayment', 'firstMissed', 'dueWhenFirstMissed', 'receivedAfterMissed', 'riskFreeRate', 'npv'])
    
# Note: pseudo values can be added in R by calling calculatePseudoValues from lendingClub.R 