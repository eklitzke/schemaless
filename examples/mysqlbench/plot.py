import csv
import sys
import optparse

import matplotlib
matplotlib.use('cairo')
from matplotlib import pyplot

def main(csv_name, opts):
	reader = iter(csv.reader(open(csv_name)))
	names = reader.next()
	data = dict((n, []) for n in names)
	for row in reader:
		for name, val in zip(names, row):
			data[name].append(float(val))

	for name in names[1:]:
		xs, ys = [], []
		for x in xrange(len(data[name])):
			xs.append(data['cumulative'][x])
			ys.append(data[name][x])
		pyplot.plot(xs, ys, label=name)
		#pyplot.scatter(xs, ys, label=name)
	pyplot.xlabel('# of records inserted')
	pyplot.ylabel('time per 10k inserts')
	pyplot.legend(loc=2)
	if opts.title:
		pyplot.title(opts.title)

	pyplot.savefig(opts.output, format='png', dpi=120)

if __name__ == '__main__':
	parser = optparse.OptionParser()
	parser.add_option('-t', '--title', default=None, help='the title to use')
	parser.add_option('-o', '--output', default='graph.png', help='what file to output to')
	opts, args = parser.parse_args()
	if len(args) != 1:
		parser.error('must specify an input file')
	main(args[0], opts)
