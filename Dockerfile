FROM python:slim

ADD src/deployer.py .
ADD requirements.txt .

RUN pip install --root-user-action=ignore --upgrade pip
RUN pip install --root-user-action=ignore -r requirements.txt

CMD ["python", "/deployer.py"]