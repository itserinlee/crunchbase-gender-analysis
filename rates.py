import pandas as pd, numpy as np
from rich import print as rprint
from plotnine import *
import os


def derivative(data, timeIndex, yIndex, outputYIndex):

    t1 = data[timeIndex].tolist()

    ## t2 is one smaller than t1
    t2 = [(t1[i] + t1[i + 1]) / 2 for i in range(len(t1) - 1)] # every index but skip the last one

    y = data[yIndex].tolist()
    v = [(y[i + 1] - y[i]) / (t1[i + 1] - t1[i]) for i in range(len(y) - 1)]

    result = {timeIndex: t2, outputYIndex: v}
    return pd.DataFrame(result)


def graphData(data: pd.DataFrame,
            timeIndex: str,
            outputYIndex: str,
            title: str,
            hexColor: str = '#69b3a2'):

    print('Making area plot.')

    p = (ggplot(
                data, aes(x = timeIndex, y = outputYIndex)
                ) +
        geom_area(fill = hexColor, alpha = 0.4) +
        geom_line(color = hexColor, size = 2) +
        geom_point(size = 3, color = hexColor) +
        ggtitle(title)
        )

    print('Saving figure.')

    filepath = 'img'
    if not os.path.exists(filepath):
        os.mkdir(filepath)
    
    ggsave(plot = p, filename = f'{outputYIndex}.png', path = filepath)



def processData(df: pd.DataFrame):

    ### male ###

    firstorder = derivative(df, 'years', 'male', 'maleVelocity')
    rprint(firstorder)
    print('')

    secondorder = derivative(firstorder, 'years', 'maleVelocity', 'maleAcceleration')
    rprint(secondorder)
    print('')
    
    graphData(df, 'years', 'male', 'Male Funding History')
    graphData(firstorder, 'years', 'maleVelocity', 'Velocity of Male Funding Per Year')
    graphData(secondorder, 'years', 'maleAcceleration', 'Acceleration of Male Funding Per Year^2')

    ### female ###

    firstorder = derivative(df, 'years', 'female', 'femaleVelocity')
    rprint(firstorder)
    print('')

    secondorder = derivative(firstorder, 'years', 'femaleVelocity', 'femaleAcceleration')
    rprint(secondorder)
    print('')

    graphData(df, 'years', 'female', 'Female Funding History')
    graphData(firstorder, 'years', 'femaleVelocity', 'Velocity of Female Funding Per Year')
    graphData(secondorder, 'years', 'femaleAcceleration', 'Acceleration of Female Funding Per Year^2')



def main():

    binnedFunding = pd.read_csv('binned_output/year_funded.csv')

    processData(binnedFunding)


if __name__ == '__main__':

    main()