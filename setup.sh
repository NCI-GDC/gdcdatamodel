git clone https://github.com/NCI-GDC/git-hooks.git 
cp -r git-hooks/*commits .git/hooks/
cp git-hooks/pre-commit .git/hooks/
rm -rf git-hooks
