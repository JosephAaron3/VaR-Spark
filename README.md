# Overview
Estimating Value-at-Risk (VAR) for all NASDAQ-listed stocks using historical data from Yahoo Finance.
Market factors are S&P 500, NASDAQ indexes, and 5-/30-year US treasury bonds. VAR is estimated via 
Monte Carlo simulation, implemented in Spark. I'm starting with Scala, and then repeating in Python,
R, and Java to compare syntax differences.