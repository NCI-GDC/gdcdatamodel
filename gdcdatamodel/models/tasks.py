from sqlalchemy import Column, Integer, String, Text, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgres import ARRAY

Base = declarative_base()


class Task(object):

    id = Column(Integer, primary_key=True)
    node_id = Column(String, nullable=False)
    runner_type = Column(String, nullable=False)
    created = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('now()'),
    )

    def __repr__(self):
       return (
           "<{cls}(runner_type='{runner_type}', node_id='{node_id}')>"
               .format(
                   cls=self.__class__.__name__,
                   node_id=self.node_id,
                   runner_type=self.runner_type,
               )
       )


class RunnerTask(Task, Base):
    __tablename__ = 'runner_tasks'
