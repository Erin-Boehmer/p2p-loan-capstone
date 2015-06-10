library(dplyr)
library(MonetDB.R)

# Combine source csv loan files into a processed file
combineLoanFiles = function() {
  a = read.csv("LoanStats3a_securev1.csv", skip=1, encoding="UTF-8", colClasses="character", as.is=TRUE)
  
  # There are two groups of records in this file
  group2 = which(a$id == 'Loans that do not meet the credit policy')
  a1 = a[1:(group2-1),]
  a1$credit_policy=1
  a2 = a[(group2+1):nrow(a),]
  a2$credit_policy=0
  # The second group has a prefix on its loan_status values
  a2$loan_status = substr(a2$loan_status, nchar("Does not meet the credit policy.  Status:")+1, nchar(a2$loan_status))
  
  b = read.csv("LoanStats3b_securev1.csv", skip=1, encoding="UTF-8", colClasses="character", as.is=TRUE)
  b$credit_policy=1
  
  c = read.csv("LoanStats3c_securev1.csv", skip=1, encoding="UTF-8", colClasses="character", as.is=TRUE)
  c$credit_policy=1
  
  d = read.csv("LoanStats3d_securev1.csv", skip=1, encoding="UTF-8", colClasses="character", as.is=TRUE)
  d$credit_policy=1
  
  df = rbind(a1,a2,b,c,d)
  
  # Remove footer rows where only the first column has a value
  df = df[df$member_id!='',]

  # Convert values like SEP-2014 to real dates
  df$issue_d = as.Date(paste0(df$issue_d, "-01"), "%b-%Y-%d")
  df$last_pymnt_d = as.Date(paste0(df$last_pymnt_d, "-01"), "%b-%Y-%d")
  df$next_pymnt_d = as.Date(paste0(df$next_pymnt_d, "-01"), "%b-%Y-%d")
  df$last_credit_pull_d = as.Date(paste0(df$last_credit_pull_d, "-01"), "%b-%Y-%d")
  
  write.table(df, "LoanStats3.csv", sep=",", qmethod="double", fileEncoding="UTF-8", row.names=FALSE)
}

# Read data from a processed csv file with one row per loan
readLoans = function(filename) {
  read.csv(filename, encoding="UTF-8", colClasses=c(issue_d="Date", last_pymnt_d="Date", next_pymnt_d="Date", last_credit_pull_d="Date"))
}

# Load loan history csv file into MonetDB
loadHistory = function(filename, tablename="history_inv") {
  dbiconn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/demo")
  #options(monetdb.debug.query=T)
  
  # We have to create the history table carefully, so that monet.read.csv can complete without error.
  histfile = "PMTHIST_INVESTOR_20150505_v2.csv"
  headers = read.csv(histfile, nrows = 500) %>%
    # Month is a keyword, so we have to manually escape it.
    rename('"Month"'=Month) %>% 
    # PublicRec is mostly integers, but there are a few * values also.
    mutate(PublicRec=as.character(PublicRec)) %>% 
    filter(FALSE) %>% collect()
  dbSendUpdate(dbiconn, "drop table history_inv")
  dbWriteTable(dbiconn, "history_inv", headers)
  monet.read.csv(dbiconn, histfile, "history_inv", create = FALSE, locked = TRUE)
}

# Merge summary of payment history with loan data
joinHistory = function() {
  conn = src_monetdb("demo")
  hist = tbl(conn, "history_inv")
  histdf = hist %>%
    # Merge amounts received from borrower and from debt collectors into received_amt
    mutate(received_amt2 = if(co==1 & pco_recovery_investors>0) pco_recovery_investors - pco_collection_fee_investors else received_amt_investors) %>%
    # Read needed columns into memory for further processing
    select(loan_id, mob, received_amt=received_amt2, due_amt=due_amt_investors, co, pbal_beg_period_investors) %>%
    collect()
  
  # Calculate summary features for each loan's history
  outcomedf = tbl_df(histdf) %>%
    group_by(loan_id) %>%
    do(analyzeHistory(.))
  
  # Join with loan table and save results for later
  loans = readLoans("LoanStats3.csv")
  joined = outcomedf %>% select(id=loan_id, firstMissed, censored, n, receivedAfterMissed, dueWhenFirstMissed)  %>% inner_join(loans, by="id")
  write.csv(joined, "joined.csv", fileEncoding = "UTF-8")
  joined
}

# Analyze history for a single loan
# Input - the payment history dataframe
# Output - summary statistics
analyzeHistory = function(df) {
  # There can be a mob 0, if the borrower sends in an extra payment before their first payment is due
  offset = 1
  if(df$mob[1]==0)
    offset = 2
  n = nrow(df)
  censored = TRUE
  firstMissed = -1
  # There might only be a mob 0, in which case we skip the loop 
  if(offset > n) {
    mobok = TRUE
  } else {
    
    # Sometimes we get multiple copies of each row in the source file
    if(n>1 && df$mob[1]==df$mob[2]) {
      copies = max(which(df$mob == df$mob[1]))
      df = df[seq(copies,n,copies),]
      n = n %/% copies
    }
    mobok = isTRUE(all.equal(df$mob[offset:n], 1:(n-offset+1)))
    
    # Find the mob for the first missed payment
    for(i in offset:n) {
      r = df$received_amt[i]
      if(is.na(r)) r = 0
      d = df$due_amt[i]
      if(is.na(d)) d = 0
      if(r + 0.01 < d) {
        firstMissed = df$mob[i]
        dueWhenFirstMissed = df$pbal_beg_period_investors[i]
        censored = FALSE
        break
      }
    }
    
  }
  if(censored) {
    receivedAfterMissed = 0
    dueWhenFirstMissed = 0
  } else {
    receivedAfterMissed = sum(df$received_amt[(firstMissed+offset):n], na.rm = TRUE)
  }
  anyco = any(df$co==1)
  data_frame(firstMissed, censored, n, receivedAfterMissed, mobok, anyco, dueWhenFirstMissed)
}