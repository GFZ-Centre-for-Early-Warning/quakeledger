# 2019-09-26

- Added the changelog
- Rewrote the xml validation tests to be convered by unittests
- Refactored the script to add the peru events
- Added handling for the peru events:
  * For the stochastic events there were no probability given in the file.
    I added a custom value of 0.1 to make it work. With better
    data we can change this.
  * For the stochastic events I added a year value to be 3000.
    In the there were no years for those and the chile ones
    just have a value from 1 to 100000.

# 2019-09-20

- Created a branch with peru events
