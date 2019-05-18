import subprocess

if __name__ == "__main__":
    subprocess.call(['sudo', 'python', '-m', 'xmlrunner', 'irods.test.test_plugin_storage_tiering' ])

