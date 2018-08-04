import re
import xgboost as xgb
from graphviz import Digraph

_NODEPAT = re.compile(r'(\d+):\[(.+)\]')
_LEAFPAT = re.compile(r'(\d+):(leaf=.+)')
_EDGEPAT = re.compile(r'yes=(\d+),no=(\d+),missing=(\d+)')
_EDGEPAT2 = re.compile(r'yes=(\d+),no=(\d+)')


def _parse_node(graph, text):
    """parse dumped node"""
    match = _NODEPAT.match(text)
    if match is not None:
        node = match.group(1)
        graph.node(node, label=match.group(2), shape='plaintext')
        return node
    match = _LEAFPAT.match(text)
    if match is not None:
        node = match.group(1)
        graph.node(node, label=match.group(2).replace('leaf=',''), shape='plaintext')
        return node
    raise ValueError('Unable to parse node: {0}'.format(text))


def _parse_edge(graph, node, text, yes_color='#0000FF', no_color='#FF0000'):
    """parse dumped edge"""
    try:
        match = _EDGEPAT.match(text)
        if match is not None:
            yes, no, missing = match.groups()
            if yes == missing:
                graph.edge(node, yes, label='yes, missing', color=yes_color)
                graph.edge(node, no, label='no', color=no_color)
            else:
                graph.edge(node, yes, label='yes', color=yes_color)
                graph.edge(node, no, label='no, missing', color=no_color)
            return
    except ValueError:
        pass
    match = _EDGEPAT2.match(text)
    if match is not None:
        yes, no = match.groups()
        graph.edge(node, yes, label='yes', color=yes_color)
        graph.edge(node, no, label='no', color=no_color)
        return
    raise ValueError('Unable to parse edge: {0}'.format(text))


def print_tree(model):
    # 带真实特征名字的用这个
    # tree = model.get_dump(fmap='xgb.fmap')[0]
    # 带虚拟特征名字的用这个
    tree = model.get_dump()[0]
    tree = tree.split()


    kwargs = {
            #'label': 'A Fancy Graph',
            'fontsize': '24',
            #'fontcolor': 'white',
            #'bgcolor': '#333333',
            #'rankdir': 'BT'
             }
    kwargs = kwargs.copy()
    #kwargs.update({'rankdir': rankdir})
    graph = Digraph(format='pdf', node_attr=kwargs,edge_attr=kwargs,engine='dot')#,edge_attr=kwargs,graph_attr=kwargs,
    #graph.attr(bgcolor='purple:pink', label='agraph', fontcolor='white')

    yes_color='#0000FF'
    no_color='#FF0000'
    for i, text in enumerate(tree):
        if text[0].isdigit():
            node = _parse_node(graph, text)
        else:
            if i == 0:
                # 1st string must be node
                raise ValueError('Unable to parse given string as tree')
            _parse_edge(graph, node, text, yes_color=yes_color,no_color=no_color)

    graph.render('XGBoost_tree.pdf')

    # graph

if __name__ == '__main__':
    # 输出树的形状
    import xgboost as xgb
    bst = xgb.Booster({'nthread':4})
    bst.load_model('./model/xgb.model')
    print_tree(bst)
    # xgb.plot_importance(bst)
    # xgb.plot_tree(bst, fmap='xgb.fmap')
