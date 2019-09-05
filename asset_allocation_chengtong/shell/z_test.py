import criteria_ct
import click






@click.group(invoke_without_command=True)
@click.pass_context
def criteria(ctx):
    pass

@criteria.command()
@click.pass_context
def func1(ctx):
    df = criteria_ct.MonthRank_Rolling()
    print('\n')
    print('JiJinChi:----------')
    print(df.dropna(subset=[df.columns[0]]))

@criteria.command()
@click.pass_context
def func2(ctx):
    df = criteria_ct.WeekRank_Rolling()
    print('\n')
    print('HuoBi: --------------------')
    print(df.dropna(subset=[df.columns[0]]))

@criteria.command()
@click.pass_context
def func3(ctx):
    df = criteria_ct.SteadyRank_Rolling()
    print('\n')
    print('WenJian: ------------------')
    print(df.dropna(subset=[df.columns[0]]))
    
@criteria.command()
@click.pass_context
def func4(ctx):
    df = criteria_ct.Month_3_Rank_Rolling()
    print('\n')
    print('ZhiNeng: -----------------')
    print(df.dropna(subset=[df.columns[0]]))

@criteria.command()
@click.pass_context
def func5(ctx):
    df = criteria_ct.update()
    print('\n')
    print('dfCore: -----------------')
    print(df)





if __name__ == '__main__':
    criteria(obj={})
