### Notes

  - Issue: When running pytest, the updated code that I wrote was not being used (it made it seem as if a new method's argument was not there). 
    - **Solution**: Re-run setup.py after making the update then use pytest. Pytest must use the setup installation if it exists??