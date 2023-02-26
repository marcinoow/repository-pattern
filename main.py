import abc
from typing import Union
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, DeclarativeBase, sessionmaker
from dataclasses import dataclass, field


@dataclass
class UserEntity:
    id: str
    name: str
    cars: list['CarEntity'] = field(default_factory=list)


@dataclass
class CarEntity:
    id: str
    brand: str
    user: Union[None, UserEntity] = None


class Base(DeclarativeBase):
    pass


class UserModel(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    name = Column(String)
    cars = relationship('CarModel', back_populates='user')


class CarModel(Base):
    __tablename__ = 'comments'
    id = Column(String, primary_key=True)
    brand = Column(String)
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship('UserModel', back_populates='cars')


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def add(self, obj):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference):
        raise NotImplementedError


class CarRepository(AbstractRepository):

    def __init__(self, session):
        self.session = session

    def add(self, car: CarEntity):
        car_model = car_entity_to_model(car)
        self.session.add(car_model)
        self.session.commit()

    def get(self, id_) -> Union[CarEntity, None]:
        car_model = self.session.query(CarModel).get(id_)
        if car_model is not None:
            return car_model_to_entity(car_model)

    def list(self) -> list[CarEntity]:
        car_models = self.session.query(CarModel).all()
        return [car_model_to_entity(car) for car in car_models]


class UserRepository(AbstractRepository):

    def __init__(self, session):
        self.session = session
        self._identity_map = dict()

    def add(self, user: UserEntity):
        self._identity_map[user.id] = user
        user_model = user_entity_to_model(user)
        self.session.add(user_model)
        self.session.commit()

    def get(self, id_: str) -> Union[UserEntity, None]:
        user_model = self.session.query(UserModel).get(id_)
        return self._get_user(user_model, user_model_to_entity)

    def list(self) -> list[UserEntity]:
        user_models = self.session.query(UserModel).all()
        return [user_model_to_entity(user) for user in user_models]

    def _get_user(self, instance, mapper_func):
        if instance is None:
            return None
        entity = mapper_func(instance)
        if entity.id in self._identity_map:
            return self._identity_map[entity.id]
        self._identity_map[entity.id] = entity
        return entity


class AbstractUnitOfWork(abc.ABC):
    users: AbstractRepository
    cars: AbstractRepository

    def __enter__(self) -> 'AbstractUnitOfWork':
        return self

    def __exit__(self, *args):
        self.rollback()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError


class MainUnitOfWork(AbstractUnitOfWork):
    def __init__(self, db_engine):
        self.db_engine = db_engine
        self.session_factory = sessionmaker(bind=db_engine)

    def __enter__(self):
        self.session = self.session_factory()
        self.users = UserRepository(self.session)
        self.cars = CarRepository(self.session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()


def car_model_to_entity(car: CarModel) -> CarEntity:
    car_entity = CarEntity(id=car.id, brand=car.brand)
    car_entity.user = UserEntity(id=car.user.id, name=car.user.name, cars=[car_entity])
    return car_entity


def car_entity_to_model(car: CarEntity) -> CarModel:
    return CarModel(id=car.id, brand=car.brand, user=UserModel(id=car.user.id, name=car.user.name))


def user_model_to_entity(user: UserModel) -> UserEntity:
    user_entity = UserEntity(id=user.id, name=user.name)
    user_entity.cars = [CarEntity(id=car.id, brand=car.brand, user=user_entity) for car in user.cars]
    return user_entity


def user_entity_to_model(user: UserEntity) -> UserModel:
    user_model = UserModel(id=user.id, name=user.name)
    user_model.cars = [CarModel(id=car.id, brand=car.brand, user=user_model) for car in user.cars]
    return user_model


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)

    main_uow = MainUnitOfWork(engine)
    with main_uow:
        print('[*] Adding objects...')
        car1 = CarEntity('car1', 'Audi')
        user1 = UserEntity('user1', 'John')
        user1.cars.append(car1)
        print(f'[*] UserEntity: {user1}')
        main_uow.users.add(user1)
        # OR
        # car1.user = user1
        # main_uow.cars.add(car1)

        print('[*] Getting objects...')
        users = main_uow.users.list()
        print(f'[*] Users entities: {users}')
        cars = main_uow.cars.list()
        print(f'[*] Cars entities: {cars}')
