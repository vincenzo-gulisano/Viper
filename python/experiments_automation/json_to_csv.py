__author__ = 'vinmas'

import csv, json, sys, math
from optparse import OptionParser


def converter(input_file,output,key):

    input = open(input_file)
    data = json.load(input)
    input.close()

    with open(output, 'w') as csvfile:
        writer = csv.writer(csvfile)  # , columns.keys())
        writer.writerow(['Timestamp', 'Watt'])

        this_sec = 0.0
        this_cons = 0
        for row in data:
            if 'Timestamp' in row.keys() and key in row.keys():
                if math.floor(float(row['Timestamp'])) == this_sec:
                    this_cons += row[key]
                else:
                    if this_sec > 0:
                        writer.writerow([str(this_sec), str(this_cons)])
                    this_sec = math.floor(float(row['Timestamp']))
                    this_cons = 0


parser = OptionParser()
parser.add_option("-i", "--input", dest="input",
                  help="JSON input file", metavar="INPUT")

parser.add_option("-o", "--output", dest="output",
                  help="Output file", metavar="OUTPUT")

parser.add_option("-k", "--key", dest="key",
                  help="Key value to extract", metavar="KEY")

(options, args) = parser.parse_args()

if options.input is None:
    print('A mandatory option (-i, --input) is missing\n')
    parser.print_help()
    exit(-1)
if options.output is None:
    print('A mandatory option (-o, --output) is missing\n')
    parser.print_help()
    exit(-1)
if options.key is None:
    print('A mandatory option (-k, --key) is missing\n')
    parser.print_help()
    exit(-1)

converter(options.input,options.output,options.key)