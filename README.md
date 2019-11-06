# Hierarchical Sorting app

## Prerequisites

- At least `Nodejs v12.12.0` or newer.
- Have `npm` or `yarn` installed

## How to run the app
1. Run the command `npm install` or `yarn install` to install dependencies.
2. Run the command `npm start` or `yarn start`.
3. Open a browser at http://localhost:3000/

# Hierarchical Sorting

For this challenge, you must create a sort function that will sort the dataset (`data.txt`, attached below) in a hierarchical way.


## Info about `data.txt`

The data contains 3 property fields and two metric fields, `net_sales` and `net_sales_units`.

The property fields define the hierarchy, and correspond to:

- `Property0`: An item's class (ex: Women's Footwear)
- `Property1`: An item's category (ex: Shoes)
- `Property2`: An item's subcategory (ex: Flats)

## Instructions

- Write code that sorts the input data in a hierarchical way (see the example below for details).
- The output should be sorted by `net_sales`, from highest to lowest.
- The output format should be pipe-delimited (like the input format).
- The level's `$total` row should always be at the top of the level, even if it's not the highest value. See example below.
- We use JavaScript or Python, but you can use any language you want (please include instructions on how to run your code).
- You are allowed to use a built-in sort function as long as it is only used to sort a list.
- Try to validate your solution by running it against the example dataset (see below).
- **Publish your solution as a secret gist and send us the link (tarball / zipfile ok too)**. Please include the sorted output along with the code.


## Example

Here's a simple input / output example (formatted as a table to make it more readable):

### input

This is the raw, unsorted input.

|property0|property1|net_sales|
|---------|---------|-----|
| bar     | $total  | -200|
| foo     | sauce   |  300|
| $total  | $total  |  200|
| bar     | sup     | -400|
| foo     | $total  |  400|
| bar     | bro     |  200|
| foo     | lettuce |  100|

### output

This is what we expect the output to be.

|property0|property1|net_sales|
|---------|---------|-----|
| $total  | $total  |  200|
| foo     | $total  |  400|
| foo     | sauce   |  300|
| foo     | lettuce |  100|
| bar     | $total  | -200|
| bar     | bro     |  200|
| bar     | sup     | -400|

A `$total` value always at the top of its group, regardless of its rank. 

The `property1` values are only sorted against the other values within their parent group (`property0`). In other words, the values in the `bar` group (which are `$total`, `bro` and `sup`) are sorted against eachother.

Same thing with `property0`, except all values are in the same group because it's the top of the hierarchy. In other words, all the `property0 = foo` rows are lower than the `property0 = bar` rows, because the `foo` total is higher than `bar`.

The raw example data files are available at the bottom of this gist.

## Bonus Points

### Support for any number of properties / metrics

Ideally, your code should not assume anything about the number of property or metric columns in the input. In our application, the input could have 10 property and 100 metrics.

The input always follows the same pattern:

- For properties, the keys are: `propertyN`
- The other keys are metrics.

### Configurable sort metric

Ideally, the algorithm should allow the user to specify which metric to sort on (instead of hardcoding to `net_sales`).

The function signature could look something like this:

```javascript
const hierarchicalSort = (data, getSortValueFn) => { ...your code here... }

hierarchicalSort(dataset, ((row) => row.net_sales))
```

In our application, we go a step further and allow the user to define the sort metric per property.

