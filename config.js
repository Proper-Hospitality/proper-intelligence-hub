// Property configurations for Proper Hospitality Intelligence Hub
const PROPERTIES = {
  smp: {
    id: 'smp',
    name: 'Santa Monica Proper Hotel',
    shortName: 'Santa Monica Proper',
    code: 'SMP',
    navTitle: ['SMP', 'Intelligence'],
    dataFile: 'data/smp.json',
    outlets: {
      primary: [
        { key: 'Calabra', label: 'Calabra', color: '#C8A951' },
        { key: 'Palma', label: 'Palma', color: '#58A6FF' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' },
        { key: 'Pool F&B', label: 'Pool F&B', color: '#F85149' }
      ],
      // Survey-specific outlets (tab 3)
      survey: [
        { key: 'survey_calabra', label: 'Calabra', commentsKey: 'calabra_comments', fields: ['Food Quality','Staff','Service','Menu'] },
        { key: 'survey_surya', label: 'Surya Spa', commentsKey: 'surya_comments', fields: ['Reservations','Therapy Rooms','Therapist','Staff'] },
        { key: 'survey_palma', label: 'Palma & Pool', commentsKey: 'palma_comments', fields: ['Food Quality','Staff','Service','Menu'], secondaryKey: 'survey_pool', secondaryLabel: 'Rooftop Pool', secondaryFields: ['Maintenance','Service','F&B'] }
      ]
    },
    compSetLabel: 'SMP vs Competitors',
    radarLabel: 'SMP Average'
  },
  austin: {
    id: 'austin',
    name: 'Austin Proper Hotel',
    shortName: 'Austin Proper',
    code: 'AUS',
    navTitle: ['Austin Proper', 'Intelligence'],
    dataFile: 'data/austin.json',
    outlets: {
      primary: [
        { key: 'Peacock', label: 'Peacock', color: '#C8A951' },
        { key: 'La Piscina', label: 'La Piscina', color: '#58A6FF' },
        { key: 'Goldies', label: "Goldie's", color: '#3FB950' },
        { key: 'In-Room Dining', label: 'In-Room Dining', color: '#D29922' }
      ],
      survey: [
        { key: 'survey_surya', label: 'Spa', commentsKey: 'surya_comments', fields: ['Reservations','Therapy Rooms','Therapist','Staff'] }
      ]
    },
    compSetLabel: 'Austin Proper vs Competitors',
    radarLabel: 'Austin Proper Average'
  }
};

// All properties in portfolio order (for landing page + portfolio view)
const PORTFOLIO_ORDER = ['smp', 'austin'];
