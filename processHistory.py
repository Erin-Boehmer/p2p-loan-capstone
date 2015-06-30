from __future__ import division
import pandas as pd
import numpy as np
import monetdb.sql
import rpy2.robjects as robjects
from rpy2.robjects.packages import importr

# Add personal R library path
robjects.r['.libPaths'](robjects.r.c(robjects.r['.libPaths'](), 'C:/Users/Nathan/Documents/R/win-library/3.2'))
fastpseudo = importr("fastpseudo")

loans = pd.read_csv('data/LoanStats3.csv.bz2', compression='bz2', low_memory=False)
loans.term = loans.term.map(lambda x: int(x.strip(' months')))

dates = pd.DataFrame(data={'issue_d': loans.issue_d.unique()})

# Parse the string format used in the loan file, and add that to the loans DataFrame
dates['issue_date'] = dates.issue_d.apply(lambda s: pd.datetime.strptime(s, '%b-%Y'))
loans = loans.merge(dates, on='issue_d')
loans.set_index('id', inplace=True)

# Generate the string date format used in the payment history file
dates['issue_d_hist'] = dates.issue_date.apply(lambda d: d.strftime('%b%Y').upper())
# Build a list of recent months for the SQL filter
recentMonths = "('" + "','".join(dates[dates.issue_date >= pd.datetools.parse('2014-01-01')].issue_d_hist) + "')"

# Monthly risk free rates for calculating NPV
riskFreeRates = pd.read_csv("data/RiskFreeRates.csv",
                        encoding='utf-8',
                        parse_dates=['observation_date'],
                        index_col=0,
                        infer_datetime_format=True)

# Amount to discount outstanding principal for Lending Club's Adjusted NAR metric
principalDiscountRates = {
    'Issued' : 0,
    'Current' : 0,
    'In Grace Period': 0.25,
    'Late (16-30 days)': 0.50,
    'Late (31-120 days)': 0.68,
    'Default': 0.89,
    'Fully Paid' : 0,
    'Charged Off': 0,
}

connection = monetdb.sql.connect(username="monetdb", password="monetdb", hostname="localhost", database="demo")

def generateOutcomes(recentMonths, old):
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
            end + 0.99*received_amt_investors as received,
            
            case when pco_recovery_investors is not null then
                pco_recovery_investors - pco_collection_fee_investors
            else
                0
            end + 0.99*(int_paid_investors + fee_paid_investors)
                - coamt_investors as receivedNAR,
                
            due_amt_investors as due,
            pbal_beg_period_investors as balance,
            period_end_lstat
        FROM history_inv
    """ 
    if old:
        sql += "WHERE received_d not in " + recentMonths
    else:
        sql += "WHERE IssuedDate in " + recentMonths
#    sql += " and loan_id='9755029'"
    cursor.execute(sql)
    
    def outputRow():
        numeratorNARdiscount = balance * principalDiscountRates[finalStatus]
        finalStatusIsComplete = finalStatus in ["Charged Off", "Default", "Fully Paid"]
        output.append((
                prev_loan_id,
                finalStatus,
                prev_mob,
                missedPayment,
                firstMissed,
                dueWhenFirstMissed,
                receivedAfterMissed,
                riskFreeRate,
                npv/investment,
                firstMissedOrLastObserved,
                (numeratorNAR-numeratorNARdiscount)/investment,
                denominatorNAR/investment,
                finalStatusIsComplete))
        

    output = []
    prev_loan_id = -1
    row = cursor.fetchone() 
    while row is not None:
        loan_id, mob, received, receivedNAR, due, balance, status = row
        if loan_id != prev_loan_id:
            # Output previous loan stats
            if prev_loan_id != -1:
                outputRow()
            
            # Set up for new loan
            loan = loans.loc[loan_id]
            if loan.term == ' 36 months':
                riskFreeRate = riskFreeRates.loc[loan.issue_date].treasury3year
            else:
                riskFreeRate = riskFreeRates.loc[loan.issue_date].treasury5year
            monthlyDiscountRate = (1 + riskFreeRate/100) ** (1/12)

            investment = loan.funded_amnt_inv
            npv = -investment
            prev_mob = -1
            prev_loan_id = loan_id
            firstMissed = -1
            firstMissedOrLastObserved = 0
            missedPayment = False
            receivedAfterMissed = 0
            dueWhenFirstMissed = 0
            numeratorNAR = 0
            denominatorNAR = 0
            
        # Skip duplicate rows
        if mob == prev_mob: 
            next
        prev_mob = mob
        
        # The history file doesn't accurately record "In Grace Period" status.  We can get it from the loan file for recent loans.
        if old:
            finalStatus = status
        else:
            finalStatus = loan.loan_status
            
        npv += received / (monthlyDiscountRate ** mob)
        numeratorNAR += receivedNAR
        denominatorNAR += balance

        totalMonths = mob
        if missedPayment:
            receivedAfterMissed += received
        else:
            firstMissedOrLastObserved = totalMonths

        if not missedPayment and due>0 and received/due < 0.99:
            firstMissed = mob
            firstMissedOrLastObserved = firstMissed
            missedPayment = True
            dueWhenFirstMissed = balance
            
        row = cursor.fetchone()

    outputRow()
    return pd.DataFrame.from_records(output, index='loan_id',
        columns=['loan_id', 'finalStatus', 'monthsObserved', 'missedPayment', 'firstMissed', 'dueWhenFirstMissed', 'receivedAfterMissed', 'riskFreeRate', 'npv', 'firstMissedOrLastObserved', 'numeratorNAR', 'denominatorNAR', 'finalStatusIsComplete'])

outcomesTrain = generateOutcomes(recentMonths, old=True)
outcomesTrain['npvPseudo'] = fastpseudo.fast_pseudo_mean(
    robjects.vectors.IntVector(np.floor(outcomesTrain.npv+1)*1000/1.3), 
    robjects.vectors.IntVector(outcomesTrain.finalStatusIsComplete),
    1000)
outcomesTrain.to_csv("data/outcomesTrain.csv")
outcomesTest = generateOutcomes(recentMonths, old=False)
outcomesTest.to_csv("data/outcomesTest.csv")