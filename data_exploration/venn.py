import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np
from matplotlib_venn import venn3, venn3_circles
def plot_venn_diagram(subsets=(73014, 11928, 2929, 15670, 2370, 1422, 851), set_labels=('EMS', 'Mental Health', 'JIMS')):
    '''
    Plot a 3-circles-venn-diagram
    :params tuple subsets: (Abc, aBc, ABc, abC, AbC, aBC, ABC)
    return: None
    :rtype: None
    '''
    fig = plt.figure(figsize=(8,8))
    v = venn3(subsets=subsets, set_labels = set_labels)
    for i in range(len(v.subset_labels)):
        v.subset_labels[i].set_text('{:,}'.format(subsets[i]))
    #v.subset_labels[0].set_text('73,014')
    #v.subset_labels[1].set_text('11,928')
    #v.subset_labels[2].set_text('2,929')
    #v.subset_labels[3].set_text('15,670')
    #v.subset_labels[4].set_text('2,370')
    #v.subset_labels[5].set_text('1,422')
    #v.subset_labels[6].set_text('851')
    plt.title("Venn Diagram")
    #plt.annotate("Unknown Set",xy=v.get_label_by_id('100').get_position() - np.array([0, 0.05]), xytext=(-70,-70), \
    #              ha='center', textcoords='offset points', bbox=dict(boxstyle='round,pad=0.5', fc='gray', alpha=0.1), \
    #              arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.5',color='gray'))
    fig.savefig("venn.png")

if __name__ == "__main__":
    plot_venn_diagram()
