import pandas as pd
import numpy as np

def loadTrainTest():
    loans = pd.read_csv('data/LoanStats3.csv.bz2', compression='bz2', low_memory=False)

    dates = pd.DataFrame(data={'issue_d': loans.issue_d.unique()})
    dates['issue_date'] = dates.issue_d.apply(lambda s: pd.datetime.strptime(s, '%b-%Y'))
    loans = loans.merge(dates, on='issue_d')

    outcomesTrain = pd.read_csv('data/outcomesTrain.csv')
    train = loans.merge(outcomesTrain, how='inner', left_on='id', right_on='loan_id')

    outcomesTest = pd.read_csv('data/outcomesTest.csv')
    test = loans.merge(outcomesTest, how='inner', left_on='id', right_on='loan_id')
    return (train, test)

def simulatePortfolios(test, scores, runs=1000):
    # Randomly select 20 of the best loans avaialable each month for 12 months
    # Return the Adjusted Net Annualized Return after 15 months (NAR excludes loans issued in the past three months)
    def simulatePortfolio():
        numerator = denominator = 0
        for month in topLoansByMonth.index.get_level_values(0).unique()[:12]:
            picks = topLoansByMonth.loc[month].sample(n=20)
            numerator += picks.numeratorNAR.sum()
            denominator += picks.denominatorNAR.sum()
        return (1 + picks.numeratorNAR.sum() / picks.denominatorNAR.sum()) ** 12 - 1

    # Find the best 5% of loans each month using the provided loan scores
    df = pd.DataFrame({'scores': scores, 'issue_date': test.issue_date, 'numeratorNAR': test.numeratorNAR, 'denominatorNAR': test.denominatorNAR })
    topLoansByMonth = df.groupby('issue_date').apply(lambda x: x.sort('scores', ascending=False).head(len(x)//20))[['numeratorNAR','denominatorNAR']]
    return np.array([simulatePortfolio() for i in range(runs)])
