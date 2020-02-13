#!/usr/bin/env python3
#
# Copyright 2016 Ricardo Garcia <r@rg3.name>
#
# To the extent possible under law, the author(s) have dedicated all copyright
# and related and neighboring rights to this software to the public domain
# worldwide. This software is distributed without any warranty.
#
# You should have received a copy of the CC0 Public Domain Dedication along
# with this software. If not, see
# <http://creativecommons.org/publicdomain/zero/1.0/>.

#
# A few different formulas for quotients.
#
import sys
import pandas as pd 

input_file = 'output/data_mandaty.csv'
output_file = 'output/data_mandaty2.csv'

# total_seats = 150

# def dhondt_formula(votes, seats):
#     return votes / (seats + 1)

# Calculates the number of seats for each party using the given quotient
# formula.
#
# - party_votes is a dictionary with party names and their number of votes.
# - seats is the total number of seats to allocate.
#
# Returns the result as a dictionary with party names and number of seats.
#
# def proportional_seats(party_votes, total_seats, quotient_formula):
#     # Calculate the quotients matrix (list in this case).
#     quot = []
#     ret = dict()
#     for p in party_votes:
#         ret[p] = 0
#         for s in range(0, total_seats):
#             q = quotient_formula(party_votes[p], s)
#             quot.append((q, p))

#     # Sort the quotients by value.
#     quot.sort(reverse=True)

#     # Take the highest quotients with the assigned parties.
#     for s in range(0, total_seats):
#         ret[quot[s][1]] += 1
#     return ret

# Loads voting data from the given stream.
#
# - Lines starting with "#" are considered comments.
# - Empty lines are ignored.
# - The first valid line contains the total number of seats.
# - Party names and number of votes are separated with semicolons.
#
# Returns a tuple (total_seats, votes) with total_seats being a number and votes
# being a dictionary.
#
# def load_data(stream):
#     total_seats = 150
#     votes = dict()
#     for line in stream:
#         fields = line.split(";")
#         (result_id,poll_date,poll_agency,party_shortname,poll_result,moving_average) = tuple(fields)
#         votes[poll_result] = int(poll_result)
#     return (result_id,poll_date,poll_agency,party_shortname,poll_result,moving_average, total_seats)

# # Load input data.
# try:
#     with open(input_file, "r") as stream:
#         (result_id,poll_date,poll_agency,party_shortname,poll_result,moving_average, total_seats) = load_data(stream)
# except IOError:
#     sys.exit("Unable to open data file")
# except ValueError:
#     sys.exit("Invalid line in data file")


# Read data from file 'filename.csv' 
# (in the same directory that your python process is based)
# Control delimiters, rows, column names with read_csv (see later) 
data = pd.read_csv(input_file) 
data.head()
# Preview the first 5 lines of the loaded data 



# # Calculate seats.
# dhondt_seats = proportional_seats(votes, total_seats, dhondt_formula)
# sl_seats = proportional_seats(votes, total_seats, sl_formula)
# modified_sl_seats = proportional_seats(votes, total_seats, modified_sl_formula)

# # Calculate percentages.
# total_votes = sum(votes.values())
# votes_pct = dict((p, votes[p] / total_votes * 100) for p in votes)
# dhondt_seats_pct = dict(
#         (p, dhondt_seats[p] / total_seats * 100) for p in dhondt_seats)
# sl_seats_pct = dict(
#         (p, sl_seats[p] / total_seats * 100) for p in sl_seats)
# modified_sl_seats_pct = dict(
#         (p, modified_sl_seats[p] / total_seats * 100)
#         for p in modified_sl_seats)

# # Write results as an HTML table.
# import locale
# sorted_parties = [(votes[p], p) for p in votes]
# sorted_parties.sort(reverse=True)
# sorted_parties = [x[1] for x in sorted_parties]

# try:
#     with open(output_file, "w") as out:
#         def xprint(something):
#             return print(something, file=out)

#         xprint("<html><head>")
#         xprint('<meta http-equiv="Content-Type" '
#                 + 'content="text/html; charset=%s" />'
#                 % (locale.getpreferredencoding(), ))
#         xprint("<title>Single voting district for %s</title>"
#                 % (input_file, ))
#         xprint('<style type="text/css">')
#         xprint("    th, td { padding: 0 .25ex; }")
#         xprint("    body { font-family: monospace; }")
#         xprint("</style>")
#         xprint("</head><body><table border=1>")
#         xprint("<tr>" 
#                 + "<th>Party</th><th>Votes</th><th>Votes %</th>"
#                 + "<th>DH seats</th><th>DH %</th>"
#                 + "<th>SL seats</th><th>SL %</th>"
#                 + "<th>MSL seats</th><th>MSL %</th>"
#                 + "</tr>"
#                 )
#         for p in sorted_parties:
#             xprint("<tr>")
#             xprint("<td>%s</td>" % (p, ))
#             xprint("<td>%s</td>" % (votes[p], ))
#             xprint("<td>%.2f</td>" % (votes_pct[p], ))
#             xprint("<td>%s</td>" % (dhondt_seats[p], ))
#             xprint("<td>%.2f</td>" % (dhondt_seats_pct[p], ))
#             xprint("<td>%s</td>" % (sl_seats[p], ))
#             xprint("<td>%.2f</td>" % (sl_seats_pct[p], ))
#             xprint("<td>%s</td>" % (modified_sl_seats[p], ))
#             xprint("<td>%.2f</td>" % (modified_sl_seats_pct[p], ))
#             xprint("</tr>")
#         xprint("</table></body></html>")
# except IOError:
#     sys.exit("Unable to write results to output file")
