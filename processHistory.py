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

mb = pd.datetools.MonthBegin()
d = pd.datetools.parse('2007-06-01')
maxDate = pd.datetime.now()
out = []
i=0
while d <= maxDate:
    out.append((i, d, d.strftime("%b-%Y"), d.strftime("%b%Y").upper()))
    d += mb
    i += 1
dates = pd.DataFrame(out, columns=('month_num', 'issue_date', 'issue_d', 'issue_d_hist'))

monthToNum = {k:int(v) for k,v in 
              dates.set_index('issue_d_hist').month_num.iteritems()}
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

def generateOutcomes(recentMonths, old=True, sample=None, id=None, printCashflow=False):
    # Note: MonetDB must be installed and populated first by calling the lendingClub.R loadHistory function
    # Raw data from http://additionalstatistics.lendingclub.com/ "Payments made to investors"
    cursor = connection.cursor()
    cursor.arraysize = 1000
    sql = """
        SELECT
            loan_id,
            mob,
            "Month",
            received_d,
            case when pco_recovery_investors is not null then
                pco_recovery_investors - pco_collection_fee_investors
            else
                0
            end + 0.99*received_amt_investors as received,
            
            case when pco_recovery_investors is not null then
                pco_recovery_investors - pco_collection_fee_investors
            else
                0
            end + 0.99*(int_paid_investors + fee_paid_investors) as receivedNAR,
            coamt_investors,
            due_amt_investors as due,
            pbal_beg_period_investors as balance,
            pbal_end_period_investors as balance_end,
            period_end_lstat
        FROM history_inv
    """ 
    if sample is not None:
        sql += ' LIMIT ' + str(sample)
    elif id is not None:
        sql += " WHERE loan_id=" + str(id)
    elif old:
        sql += " WHERE IssuedDate not in " + recentMonths
    else:
        sql += " WHERE IssuedDate in " + recentMonths
    cursor.execute(sql)
    
    outputLabels = (
        'loan_id',
        'finalStatus',
        'monthsObserved',
        'missedPayment',
        'firstMissed',
        'dueWhenFirstMissed',
        'receivedAfterMissed',
        'riskFreeRate',
        'npv',
        'npvForecast',
        'firstMissedOrLastObserved',
        'numeratorNAR',
        'denominatorNAR',
        'finalStatusIsComplete',
        'irrForecast',
        #'cashflow'
    )
    def outputRow():
        #NOTE: The fields of the db row, such a loan_id, are no longer available when this is called.
        discountRate = principalDiscountRates[finalStatus]
        numeratorNARdiscount = prev_balance_end * discountRate
        cashflowLen = len(cashflow)
        while cashflow[cashflowLen-1]==0: cashflowLen -= 1
        cashflow.resize(max(cashflowLen, receivedMonthIndex+1))
        npv = np.npv(monthlyDiscountRate-1, cashflow)
        if printCashflow: print(cashflow)
        if finalStatus != 'Charged Off':
            cashflow[receivedMonthIndex] += prev_balance_end * (1-discountRate) / prev_investment
        if printCashflow: print(cashflow)
        npvForecast = np.npv(monthlyDiscountRate-1, cashflow)
        irrForecast = (1+np.irr(cashflow))**12 - 1
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
                npv,
                npvForecast,
                firstMissedOrLastObserved,
                (numeratorNAR-numeratorNARdiscount)/prev_investment,
                denominatorNAR/prev_investment,
                finalStatusIsComplete,
                irrForecast,
                #cashflow
            ))        

    output = []
    prev_loan_id = -1
    row = cursor.fetchone() 
    while row is not None:
        loan_id, mob, month, received_d, received, receivedNAR, coamt, due, balance, balance_end, status = row
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
            prev_mob = -1
            prev_loan_id = loan_id
            prev_investment = investment
            firstMissed = -1
            firstMissedOrLastObserved = 0
            missedPayment = False
            receivedAfterMissed = 0
            dueWhenFirstMissed = 0
            numeratorNAR = 0
            denominatorNAR = 0
            cashflow = np.zeros(100)
            cashflow[0] = -1
            firstMonthNum = monthToNum[month]
            
        # Skip duplicate rows
        if mob == prev_mob: 
            next
        prev_mob = mob
        prev_balance = balance
        prev_balance_end = balance_end
        prev_received = received
        
        # The history file very rarely records "In Grace Period" status.  We can get it from the loan file for recent loans.
        if not old and status == 'Current' and loan.loan_status == 'In Grace Period':
            finalStatus = loan.loan_status
        else:
            finalStatus = status
            
        numeratorNAR += receivedNAR - coamt
        denominatorNAR += balance
        if received_d is not None:
            receivedMonthIndex = monthToNum[received_d]-firstMonthNum+1
        else:
            receivedMonthIndex = monthToNum[month]-firstMonthNum+1
        cashflow[receivedMonthIndex] += received/investment

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
    return pd.DataFrame.from_records(output, index='loan_id', columns=outputLabels)

outcomesTrain = generateOutcomes(recentMonths, old=True)
outcomesTrain['npvPseudo'] = fastpseudo.fast_pseudo_mean(
    robjects.vectors.IntVector(np.floor((outcomesTrain.npv+1)*100/1.3)), 
    robjects.vectors.IntVector(outcomesTrain.finalStatusIsComplete),
    1000)
outcomesTrain.to_csv("data/outcomesTrain.csv")
outcomesTest = generateOutcomes(recentMonths, old=False)
outcomesTest.to_csv("data/outcomesTest.csv")