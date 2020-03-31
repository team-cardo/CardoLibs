from setuptools import find_packages, setup

setup(
    name='CardoLibs',
    version='1.9.53',
    packages=find_packages(),
    url='',
    license='',
    author='CardoTeam',
    author_email='',
    description='',
    install_requires=[
        'CardoExecutor==1.3.*',
        'requests',
        'python-dateutil',
        'xlrd',
        'elasticsearch==5.4.0'
    ],
    package_data={'': ['test_table.xlsx', 'fixed_empty.xlsx']},
    include_package_data=True
)
