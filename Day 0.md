#### Install Homebrew & python on MAC: 
1. If you don't have Homebrew installed, open the Terminal app 
(you can find it in the Utilities folder in Applications or use Spotlight search). 
Then, copy and paste the following command and press Enter:

``` bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
2. Install Python: Once Homebrew is installed, simply run the following command in your Terminal:
``` bash
brew install python
```
3. Verify the Installation:
``` bash
python3 --version
```

#### Create & Activate my virtual environment:
``` bash
python3 -m venv <venv name>
source <venv name>/bin/activate
```
