# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/13 09:58
@function:
"""
import jieba

if __name__ == '__main__':
    content = "Hsoleil毕业于淮北师范大学，后在清华大学继续深造"

    result = jieba.cut(content, True)
    print(list(result), type(result))

    # 不会做关键词进行进一步的切分 二次组合
    result1 = jieba.cut(content, False)
    print(list(result1), type(result1))

    # 搜索引擎模式 允许二次组合
    result2 = jieba.cut_for_search(content)
    print(",".join(result2))