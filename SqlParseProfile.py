# -*- coding: utf-8 -*-
# !/usr/bin/env python

"""
-------------------------------------------------
   Desc : 为SQL提取基本的元信息：
            1. 指标
            2. 依赖的数据表
            3. 维度
            4. 维度组合
   File : sql_profile.py
-------------------------------------------------
pip install sqlparse
仅做词法解析，不做语法校验
支持hive,presto的维度，指标，表名的解析
"""

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML


class SqlParseProfile(object):
    def __init__(self, sql):
        self.sql_statement = sqlparse.parse(sql)[0]

    def get_sql_profile(self):
        """
        为SQL提取基本的元信息：
        1. 指标
        2. 依赖的数据表
        3. 维度
        4. 维度组合
        :return:
        """
        dims=self._get_dimensions()
        metrics=self._get_metrics()
        dim_sets=self._get_dimension_combinations()
        dependency_table=self._get_dependencies()
        return {"dims":dims,"metrics":metrics,"dim_sets":dim_sets,"dependency_table":dependency_table}


    def _get_metrics(self):
        """
        获取指标
        :return: metrics 指标
        """
        metrics = []
        for identifier_list in self.sql_statement:  # 获取sql的statement
            # 通过解析sqlparse.sql.IdentifierList来获取指标项
            if isinstance(identifier_list, sqlparse.sql.IdentifierList):
                for identifier in identifier_list:
                    # 通过解析sqlparse.sql.Identifier来获取指标
                    if isinstance(identifier, sqlparse.sql.Identifier):
                        for metric in identifier:
                            # 通过解析sqlparse.sql.Identifier来获取指标
                            if isinstance(metric, sqlparse.sql.Identifier):
                                metrics.append(str(metric))
        return metrics

    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is sqlparse.tokens.DML and item.value.upper() == 'SELECT':
                return True
        return False

    def extract_from_part(self, parsed):
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if self.is_subselect(item):
                    for x in self.extract_from_part(item):
                        yield x
                elif item.ttype is sqlparse.tokens.Keyword:
                    raise StopIteration
                else:
                    yield item
            elif item.ttype is sqlparse.tokens.Keyword and item.value.upper() == 'FROM':
                from_seen = True

    def _get_dimensions(self):
        """
        获取维度
        :return: dimensions 维度
        """
        dimensions = []
        for identifier_list in self.sql_statement:  # 获取sql的statement
            # 通过解析sqlparse.sql.IdentifierList来获取维度
            if isinstance(identifier_list, sqlparse.sql.IdentifierList):
                for identifier in identifier_list:
                    # 通过解析sqlparse.sql.Identifier来获取维度
                    if isinstance(identifier, sqlparse.sql.Identifier):
                        for dimension in identifier:
                            # 通过解析sqlparse.sql.Token来获取维度
                            if str(dimension.ttype) == 'Token.Name':
                                dimensions.append(str(dimension))
        return dimensions

    def _get_dimension_combinations(self):
        """
        获取维度组合
        :return:
        """
        dimension_combinations = set()



        for function in self.sql_statement:
            if isinstance(function, sqlparse.sql.Function):

                for parenthesis in function:

                    if isinstance(parenthesis, sqlparse.sql.Parenthesis):

                        for parenthesis1 in parenthesis:

                            if isinstance(parenthesis1, sqlparse.sql.Parenthesis):
                                if (str(parenthesis1)=="()"):
                                    dimension_combinations.add(str(parenthesis1))
                                else:
                                    dimension_combinations.add(
                                        str(parenthesis1).replace("(","").replace(")",""))


                                # for dimension_combination in parenthesis1:
                                #     print dimension_combination,"44444444444"
                                #     if isinstance(dimension_combination, sqlparse.sql.Identifier) or isinstance(
                                #             dimension_combination,
                                #             sqlparse.sql.IdentifierList):
                                #         dimension_combinations.add(str(dimension_combination))

                            # else:
                                # print parenthesis1


        return dimension_combinations

    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

    def is_next(self, parsed):
        for i in parsed:
            if isinstance(i, sqlparse.sql.Parenthesis):
                return True
        return False

    def extract_from_part(self, parsed):
        from_seen = False
        tag = False
        for item in parsed.tokens:
            if str(item.ttype) not in ('Token.Text.Whitespace', 'Token.Text.Whitespace.Newline'):
                if from_seen and item.ttype is None and isinstance(item, sqlparse.sql.Identifier):
                    # print 'item', type(item), item
                    if not self.is_next(item) and tag:
                        yield item
                        continue
                    for i in item.tokens:
                        # print 'i', type(i), i
                        if isinstance(i, sqlparse.sql.Parenthesis):
                            if self.is_subselect(i):
                                for x in self.extract_from_part(i):
                                    # print type(x), x, x.ttype
                                    yield x
                            elif i.ttype is Keyword:
                                raise StopIteration
                        else:
                            if not isinstance(i, sqlparse.sql.Identifier):
                                yield i
                elif item.ttype is Keyword and item.value.upper() == 'FROM':
                    from_seen = True
                tag = item.ttype is Keyword and item.value.upper() == 'FROM'

    def extract_table_identifiers(self, token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_name()
            elif isinstance(item, Identifier):
                if item.get_parent_name() is not None:
                    yield item.get_parent_name() + '.' + item.get_name()
                else:
                    yield item.get_name()
            # It's a bug to check for Keyword here, but in the example
            # above some tables names are identified as keywords...
            elif item.ttype is Keyword:
                yield item.value

    def _get_dependencies(self):
        """
        获取依赖的数据表
        :return: dependencies 依赖表
        """
        stream = self.extract_from_part(self.sql_statement)
        return set(self.extract_table_identifiers(stream))


if __name__ == '__main__':
    sql = """
            select prov,city,count(1) as cnt from tmp.test group by prov,city grouping_sets((),(prov),(prov,city));
        """

    sql_profile = SqlParseProfile(sql)
    print(sql_profile.get_sql_profile())
