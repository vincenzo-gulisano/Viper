import matplotlib

matplotlib.use('Agg')
from matplotlib import rcParams
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

matplotlib.rcParams.update({'font.size': 8})


def create_overview_graph(storm_keys, viper_keys, keys_marker, keys_legend, ops, storm_throughput, storm_latency,
                          storm_consumption,
                          viper_throughput, viper_latency, viper_consumption, ops_ticks, ops_labels, throughput_ticks,
                          throughput_ticks_labels, latency_ticks, latency_ticks_labels, consumption_ticks,
                          consumption_ticks_labels, outFile):
    rcParams.update({'figure.autolayout': False})
    pp = PdfPages(outFile)

    f = plt.figure()
    f.set_size_inches(20, 10)

    # plot_lines = []
    # labels = []

    ax = plt.subplot(3, 2, 1)
    for key in storm_keys:
        plt.plot(ops, storm_throughput[key], marker=keys_marker[key], label=keys_legend[key])
        # plot_lines.append(lines)
        # labels.append(keys_legend[key])
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.xticks(ops_ticks, [])
    plt.yticks(throughput_ticks, throughput_ticks_labels)
    plt.grid(True)
    plt.ylabel('Throughput (t/s)')
    plt.title('Shared-nothing')
    plt.legend(loc='upper left', fontsize=7, ncol=2, columnspacing=0.4, labelspacing=0.1)
    for axis in ['top', 'bottom', 'left', 'right']:
        ax.spines[axis].set_linewidth(0.4)

    ax = plt.subplot(3, 2, 3)
    for key in storm_keys:
        plt.plot(ops, storm_latency[key], marker=keys_marker[key])
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.xticks(ops_ticks, [])
    plt.yticks(latency_ticks, latency_ticks_labels)
    plt.grid(True)
    plt.ylabel('Latency (ms)')
    ax.title.set_visible(False)
    for axis in ['top', 'bottom', 'left', 'right']:
        ax.spines[axis].set_linewidth(0.4)

    ax = plt.subplot(3, 2, 5)
    for key in storm_keys:
        plt.plot(ops, storm_consumption[key], marker=keys_marker[key])
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.yticks(consumption_ticks, consumption_ticks_labels)
    plt.xticks(ops_ticks, ops_labels)
    plt.grid(True)
    plt.xlabel('# of Operators')
    plt.ylabel('Consumption (J/t)')
    ax.title.set_visible(False)
    for axis in ['top', 'bottom', 'left', 'right']:
        ax.spines[axis].set_linewidth(0.4)

    ax = plt.subplot(3, 2, 2)
    for key in viper_keys:
        plt.plot(ops, viper_throughput[key], marker=keys_marker[key])
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.xticks(ops_ticks, [])
    plt.yticks(throughput_ticks, [])
    plt.grid(True)
    plt.title('Hybrid')
    for axis in ['top', 'bottom', 'left', 'right']:
        ax.spines[axis].set_linewidth(0.4)

    ax = plt.subplot(3, 2, 4)
    for key in viper_keys:
        plt.plot(ops, viper_latency[key], marker=keys_marker[key])
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.xticks(ops_ticks, [])
    plt.yticks(latency_ticks, [])
    plt.grid(True)
    ax.title.set_visible(False)
    for axis in ['top', 'bottom', 'left', 'right']:
        ax.spines[axis].set_linewidth(0.4)

    ax = plt.subplot(3, 2, 6)
    for key in viper_keys:
        plt.plot(ops, viper_consumption[key], marker=keys_marker[key])
    plt.subplots_adjust(hspace=.0)
    plt.subplots_adjust(wspace=.0)
    plt.xticks(ops_ticks, ops_labels)
    plt.yticks(consumption_ticks, [])
    plt.grid(True)
    plt.xlabel('# of Operators')
    ax.title.set_visible(False)
    for axis in ['top', 'bottom', 'left', 'right']:
        ax.spines[axis].set_linewidth(0.4)

    plt.subplots_adjust(left=0.16, right=0.99, top=0.93, bottom=0.1)
    plt.close()
    pp.savefig(f)
    pp.close()

    # pp_legend = PdfPages(outFile + '.legend.pdf')
    # f_legend = plt.figure()
    # plt.figlegend(plot_lines, labels, loc='lower center', ncol=5, labelspacing=0.)
    #
    # pp_legend.savefig(f_legend)
    # pp_legend.close()

# def create_overview_graph(ops, storm_throughput, storm_latency, storm_consumption, viper_through_latency,
#                           viper_consumption, ops_labels, throughput_ticks, throughput_ticks_labey_ticks,
#                           latency_ticks_labels, consumption_ticks, consumption_ticks_labels, out


# create_overview_graph([1, 2, 4, 6], [10, 12, 14, 16], [10, 8, 6, 4], [5, 5, 5, 5], [10, 15, 20, 25], [5, 3, 2, 1],
#                       [3, 3, 3, 3], [0, 1, 2, 4, 6, 7], ['', '1', '2', '4', '6', ''], [0, 5, 10, 15, 20],
#                       ['0', '5', '10', '15', '20'], [0, 5, 10, 12], ['0', '5', '10', ''], [0, 2, 4, 6, 8, 10, 12],
#                       ['0', '2', '4', '6', '8', '10', ''], '/Users/vinmas/Desktop/test.pdf')
# import numpy as np
# import matplotlib.pyplot as plt
# import matplotlib.ticker as tic
#
# fig = plt.figure()
#
# x = np.arange(100)
# y = 3.*np.sin(x*2.*np.pi/100.)
#
# for i in range(5):
#     temp = 510 + i
#     ax = plt.subplot(temp)
#     plt.plot(x,y)
#     plt.subplots_adjust(hspace = .0)
#     temp = tic.MaxNLocator(3)
#     ax.yaxis.set_major_locator(temp)
#     ax.set_xticklabels(())
#     ax.title.set_visible(False)
#
# plt.show()
