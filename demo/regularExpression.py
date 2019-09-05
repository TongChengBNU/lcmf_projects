import re

name = 'TongCheng'
html = '<H1>Chapter 1 - 介绍正则表达式</H1>'
repetition = 'Is is the cost of of gasoline going up up'
sentence = 'With once upon a time'

regularExpression = '[A-z]? once'
# begin from head
# res = re.match(regularExpression, sentence)
# search the whole string
res = re.search(regularExpression, sentence)


if __name__ == '__main__':
    if res is None:
        print('None')
    else:
        index = res.span()
        print(index)
        print(res.string[index[0]: index[1]])