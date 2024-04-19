# Ecommerce Website Scraping and Loading Framework
*(An Attempt)*
</br>
This is an attempt at making a framework to be able to scrape data from select ecommerce website/applications and further load the data into a snowflake datawarehouse. The key principle of the framework is that it is scalable for daily use

This is built purely for educational purposes to be able to explore the options to have competitive data on pricing for benchmark purposes. While the key outcome of the extraction is to be able to conduct benchmarking of price. Other opportunities can also be explored w.r.t analysis of competitive information to shape some assortment/raneg related strategies of a business (grocery retail). 

## Folder Stucture
```bash
├───carrefour
    └───__init__.py
    └───carrefour_dev.py
    └───carrefour_menu.py
    └───carrefour_mt.py
    └───carrefour_st.py
├───credentials
    └───__init__.py
    └───credential_manager.py
├───data
├───data_processor
    └───__init__.py
    └───data_processor.py
├───db
    └───__init__.py
    └───snowflake_loader.py
├───spinneys
    └───__init__.py
    └───spinneys_dev.py
    └───spinneys_mt.py
├───state_manager
    └───__init__.py
    └───state_manager.py
├───utils
    └───__init__.py
    └───alert_manager.py
    └───config_manager.py
    └───logger.py
    └───utils.py
├───.gitignore
├───app.log
├───config.ini
├───main.py
├───menu.csv
├───README.md
```

## Task List
- [ ] Add proxy hopping
- [ ] Configure alert manager
- [ ] Capability to handle table drift
- [ ] Containerization
- [ ] Add extra ecommerce source 
