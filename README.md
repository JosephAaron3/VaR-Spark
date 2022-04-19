# Overview
Estimating Value-at-Risk (VaR) for all NASDAQ-listed stocks using historical data from Google 
Finance. Market factors are prices for SPDR Gold, iShares US Credit Bond ETF, and iShares 20 Plus 
Year Treasury Bond ETF. VaR is estimated via Monte Carlo simulation, and implemented in Spark. I'm 
starting with Scala, and then repeating in Python and R to compare syntax and overhead differences.

This is based on a case study in [1] and the data they use (which can be found on the book's repo). 
The following files are invalid and need to be removed from the provided dataset:
CVCY.csv,
DISCB.csv,
GOODO.csv,
LJPC.csv,
UNTY.csv

### References
[1] Ryza, S., Laserson, U., Owen, S., & Wills, J. (2017). Advanced Analytics with Spark (2nd ed.). O’Reilly.