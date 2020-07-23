import setuptools

setuptools.setup(
    name="cys-snapshot",
    version="0.1.0",
    author="Rudy Williams",
    author_email="rudysw05@knights.ucf.edu",
    description="A package and CLI tool for automating the number crunching for monthly reports at CYS",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": [
            "shelter=snapshot.cli.shelter_snapshot:shelter_cli",
            "nonres=snapshot.cli.nonres_snapshot:nonres_cli",
            "snap=snapshot.cli.snap_snapshot:snap_cli",
            "tlp=snapshot.cli.tlp_snapshot:tlp_cli",
            "jac=snapshot.cli.jac_snapshot:jac_cli",
            "snapshot=snapshot.cli.snapshot:all_cli",
        ]
    },
    install_requires=["pandas", "openpyxl", "xlrd", "PyYaml"],
    include_package_data=True,
)
