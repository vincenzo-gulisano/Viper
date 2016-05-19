import matplotlib

matplotlib.use('Agg')
import numpy
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

matplotlib.rcParams.update({'font.size': 8})


def x(output_file):
    rcParams.update({'figure.autolayout': False})
    pp = PdfPages(output_file)

    f = plt.figure()
    f.set_size_inches(3.33334, 3.33334 * 2 / 3)

    storm_operators = ['I', 'Op-M', 'Op', 'S-M', 'S']
    viper_operators = ['I', 'Op', 'S']

    # StatefulVehicleEnteringNewSegment
    storm_operators_costs = [4.91,6.81,4.62,6.98,0.58]
    storm_operators_ptps = [0.44,0.61,0.41,0.21,0.02]
    viper_operators_costs = [4,2.76,1.39]
    viper_operators_ptps = [0.58,0.46,0.05]

    # StatelessForwardPositionReportsOnly
    storm_operators_costs = [6.38, 8.24, 8.97, 8.58, 0.7]
    storm_operators_ptps = [0.44, 0.57, 0.62, 0.58, 0.05]
    viper_operators_costs = [4.79, 5.33, 1.08]
    viper_operators_ptps = [0.56, 0.62, 0.12]

    # StatelessForwardStoppedCarsOnly
    storm_operators_costs = [4.33, 5.72, 0.45, 15.4, 3.12]
    storm_operators_ptps = [0.49, 0.64, 0.05, 0, 0]
    viper_operators_costs = [3.6, 0.72, 5.86]
    viper_operators_ptps = [0.51, 0.1, 0]

    # StatefulSegmentAverageSpeed
    storm_operators_costs = [3.9,5.45,1.54,8.42,0.69]
    storm_operators_ptps = [0.45,0.62,0.18,0,0]
    viper_operators_costs = [3.64,1.7,1.41]
    viper_operators_ptps = [0.54,0.25,0]

    # StatefulVehicleEnteringNewSegment
    storm_operators_costs = [4.91,6.81,4.62,6.98,0.58]
    storm_operators_ptps = [0.44,0.61,0.41,0.21,0.02]
    viper_operators_costs = [4,2.76,1.39]
    viper_operators_ptps = [0.58,0.46,0.05]

    ax = plt.subplot(2, 2, 1)
    plt.grid(True, zorder=0)
    plt.bar(numpy.arange(len(storm_operators)), storm_operators_costs, 0.35, align='center', zorder=3)
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.title('SN')
    plt.xticks(numpy.arange(len(storm_operators)), [])
    plt.ylim([0, 20])
    plt.yticks([0, 4, 8, 12, 16, 20], ['0', '4', '8', '12', '16', '20'])
    plt.ylabel('Cost\n(microseconds)')
    for i in numpy.arange(len(storm_operators)):
        ax.text(i, storm_operators_costs[i] + 1, str("{0:.2f}".format(storm_operators_costs[i])), color='blue',
                fontsize=7, verticalalignment='bottom', horizontalalignment='center', rotation='vertical')

    ax = plt.subplot(2, 2, 2)
    plt.grid(True, zorder=0)
    plt.bar(numpy.arange(len(viper_operators)), viper_operators_costs, 0.35 / 2, align='center', zorder=3)
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.title('SM')
    plt.xticks(numpy.arange(len(viper_operators)), [])
    plt.ylim([0, 20])
    plt.yticks([0, 4, 8, 12, 16, 20], [])
    for i in numpy.arange(len(viper_operators)):
        ax.text(i, viper_operators_costs[i] + 1, str("{0:.2f}".format(viper_operators_costs[i])), color='blue',
                fontsize=7, verticalalignment='bottom', horizontalalignment='center', rotation='vertical')

    ax = plt.subplot(2, 2, 3)
    plt.grid(True, zorder=0)
    plt.bar(numpy.arange(len(storm_operators)), storm_operators_ptps, 0.35, align='center', zorder=3)
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.ylim([0, 1])
    plt.xticks(numpy.arange(len(storm_operators)), storm_operators, rotation='vertical')
    plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1.0], ['0', '0.2', '0.4', '0.6', '0.8'])
    plt.ylabel('Processing\nTime (%)')
    for i in numpy.arange(len(storm_operators)):
        ax.text(i, storm_operators_ptps[i] + 0.05, str("{0:.2f}".format(storm_operators_ptps[i])), color='blue',
                fontsize=7, verticalalignment='bottom', horizontalalignment='center', rotation='vertical')

    ax = plt.subplot(2, 2, 4)
    plt.grid(True, zorder=0)
    plt.bar(numpy.arange(len(viper_operators)), viper_operators_ptps, 0.35 / 2, align='center', zorder=3)
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.ylim([0, 1])
    plt.yticks([0, 0.2, 0.4, 0.6, 0.8, 1.0], [])
    plt.xticks(numpy.arange(len(viper_operators)), viper_operators, rotation='vertical')
    for i in numpy.arange(len(viper_operators)):
        ax.text(i, viper_operators_ptps[i] + 0.05, str("{0:.2f}".format(viper_operators_ptps[i])), color='blue',
                fontsize=7, verticalalignment='bottom', horizontalalignment='center', rotation='vertical')

    plt.subplots_adjust(left=0.16, right=0.99, top=0.90, bottom=0.15)
    plt.close()
    pp.savefig(f)
    pp.close()


x('/Users/vinmas/Desktop/x.pdf')
