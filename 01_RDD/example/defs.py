# -*- coding: utf-8 -*-
"""
@author: Darren
@time: 2023/7/13 10:42
@function:
"""
import jieba


def contect_jieba(data):

    """
    通过jieba进行分词操作
    :param data:
    :return:
    """
    search = jieba.cut_for_search(data)

    l = list()
    for word in search:
        l.append(word)
    return l


def filter_words_data(data):
    return data not in ['谷', '帮', '客']


def append_words(data):
    """
    修订关键词的内存
    :param data:
    :return:
    """
    if data == '传智播': data = '传智播客'
    if data == '搏学': data = '搏学谷'
    if data == '院校': data = '院校帮'
    return (data, 1)


def extract_and_word(data):
    """
    传入的数据是一个元组(user_id,search_data)
    :param data:
    :return:
    """
    user_id = data[0]
    user_content = data[1]
    words = contect_jieba(user_content)

    return_list = list()
    for word in words:
        if filter_words_data(word):
            tuple_ = (user_id+"_"+append_words(word)[0], 1)
            return_list.append(tuple_)
    return return_list


