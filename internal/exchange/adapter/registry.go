package adapter

type Registry struct {
	factories map[Route]Factory
}

func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[Route]Factory),
	}
}

func (r *Registry) Register(route Route, factory Factory) {
	if r.factories == nil {
		r.factories = make(map[Route]Factory)
	}
	r.factories[route] = factory
}

func (r *Registry) factory(route Route) (Factory, error) {
	if r == nil || r.factories == nil {
		return nil, RouteUnsupported(route)
	}
	factory := r.factories[route]
	if factory == nil {
		return nil, RouteUnsupported(route)
	}
	return factory, nil
}

func (r *Registry) CredentialValidator(route Route) (CredentialValidator, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.CredentialValidator()
}

func (r *Registry) AccountSnapshotReader(route Route) (AccountSnapshotReader, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.AccountSnapshotReader()
}

func (r *Registry) SymbolRulesReader(route Route) (SymbolRulesReader, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.SymbolRulesReader()
}

func (r *Registry) OrderExecutor(route Route) (OrderExecutor, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.OrderExecutor()
}

func (r *Registry) OrderCapabilityProvider(route Route) (OrderCapabilityProvider, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.OrderCapabilityProvider()
}

func (r *Registry) OrderStateReader(route Route) (OrderStateReader, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.OrderStateReader()
}

func (r *Registry) OrderCanceller(route Route) (OrderCanceller, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	return factory.OrderCanceller()
}

func (r *Registry) UserDataStream(route Route) (UserDataStream, error) {
	factory, err := r.factory(route)
	if err != nil {
		return nil, err
	}
	streamFactory, ok := factory.(UserDataStreamFactory)
	if !ok {
		return nil, CapabilityUnsupported("user_data_stream")
	}
	return streamFactory.UserDataStream()
}
