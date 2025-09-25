import * as asset360 from '../../../../pkg';
const { expect } = require('chai');

const SCHEMA_YAML = `
id: https://example.org/test
name: test
default_prefix: ex
prefixes:
  ex:
    prefix_reference: http://example.org/
classes:
  Person:
    slots:
      - name
      - aliases
      - role
slots:
  name:
    range: string
  aliases:
    range: string
    multivalued: true
  role:
    range: PersonRole
enums:
  PersonRole:
    permissible_values:
      manager: {}
`;

const PERSON_JSON = JSON.stringify({ name: 'Alice', aliases: ['Al'], role: 'manager' });

describe('LinkMLInstance wasm bindings', () => {
  before(async () => {
    if (typeof asset360.init === 'function') {
      await asset360.init();
    }
  });

  it('creates and inspects instances via wasm', () => {
    const view = asset360.loadSchemaView(SCHEMA_YAML);
    const instance = view.loadInstanceFromJson('Person', PERSON_JSON);

    expect(instance.kind()).to.equal('object');
    expect(instance.className()).to.equal('Person');

    const keys = instance.keys().sort();
    expect(keys).to.deep.equal(['aliases', 'name', 'role']);

    const aliasList = instance.get('aliases');
    expect(aliasList, 'aliases slot').to.not.be.undefined;
    expect(aliasList!.kind()).to.equal('list');
    expect(aliasList!.length()).to.equal(1);

    const aliasZero = aliasList!.at(0);
    expect(aliasZero, 'first alias').to.not.be.undefined;
    expect(aliasZero!.scalarValue()).to.equal('Al');

    const classViewHandle = instance.classView();
    expect(classViewHandle).to.not.be.undefined;
    expect(classViewHandle!.name()).to.equal('Person');

    const aliasSlotView = aliasList!.slotView();
    expect(aliasSlotView).to.not.be.undefined;
    expect(aliasSlotView!.name()).to.equal('aliases');
    const roleValue = instance.get('role');
    expect(roleValue).to.not.be.undefined;
    const roleSlotView = roleValue!.slotView();
    expect(roleSlotView).to.not.be.undefined;
    expect(roleSlotView!.name()).to.equal('role');

    const slotHandles = classViewHandle!.slotViews();
    const roleSlot = slotHandles.find((slot) => slot.name() === 'role');
    expect(roleSlot).to.not.equal(undefined);
    const roleInfos = roleSlot!.rangeInfos();
    expect(roleInfos.length).to.be.greaterThan(0);
    const inlineModes = roleInfos.map((info) => info.slotInlineMode());
    expect(inlineModes).to.include('primitive');

    const rawPlain = instance.toPlainJson() as unknown;
    const plainValue =
      typeof rawPlain === 'string'
        ? (JSON.parse(rawPlain) as { name: string; aliases: string[]; role: string })
        : (rawPlain as { name: string; aliases: string[]; role: string } | Map<string, unknown>);
    const plain =
      plainValue instanceof Map
        ? (Object.fromEntries(plainValue) as { name: string; aliases: string[]; role: string })
        : (plainValue as { name: string; aliases: string[]; role: string });
    expect(plain.name).to.equal('Alice');
    expect(plain.aliases).to.deep.equal(['Al']);
    expect(plain.role).to.equal('manager');

    const navigated = instance.navigate(['aliases', '0']);
    expect(navigated, 'navigate result').to.not.be.undefined;
    expect(navigated!.scalarValue()).to.equal('Al');
  });

  it('inspects schema views via wasm', () => {
    const view = asset360.loadSchemaView(SCHEMA_YAML);

    const schemaId = view.primarySchemaId() ?? '';
    expect(schemaId).to.equal('https://example.org/test');

    const classIds = view.classIds();
    expect(classIds).to.include('Person');

    const enumIds = view.enumIds();
    expect(enumIds).to.include('PersonRole');

    const classView = view.classView(schemaId, 'Person');
    expect(classView).to.not.equal(undefined);
    expect(classView!.name()).to.equal('Person');

    const slotHandles = classView!.slotViews();
    const roleSlot = slotHandles.find((slot) => slot.name() === 'role');
    expect(roleSlot).to.not.equal(undefined);

    const slotView = view.slotView(schemaId, 'role');
    expect(slotView).to.not.equal(undefined);
    expect(slotView!.definition()).to.not.equal(null);

    const rangeEnum = roleSlot!.rangeEnum();
    expect(rangeEnum).to.not.equal(undefined);
    expect(rangeEnum!.name()).to.equal('PersonRole');

    const slotRangeEnum = slotView!.rangeEnum();
    expect(slotRangeEnum).to.not.equal(undefined);
    expect(slotRangeEnum!.name()).to.equal('PersonRole');

    const enumView = view.enumView(schemaId, 'PersonRole');
    expect(enumView).to.not.equal(undefined);
    expect(enumView!.permissibleValueKeys()).to.deep.equal(['manager']);
  });

  it('resolves schema imports via addSchemaStrWithImportRef', () => {
    const importerYaml = `id: https://example.org/importer
name: importer
imports:
  - https://example.org/personinfo.yaml
`;
    const view = asset360.loadSchemaView(importerYaml);
    const unresolvedBefore = view.getUnresolvedSchemaRefs() as Array<[string, string]>;
    expect(unresolvedBefore).to.deep.equal([
      ['https://example.org/importer', 'https://example.org/personinfo.yaml'],
    ]);

    const importYaml = `id: https://example.org/personinfo
name: personinfo
`;
    const inserted = view.addSchemaStrWithImportRef(
      importYaml,
      'https://example.org/importer',
      'https://example.org/personinfo.yaml',
    );
    expect(inserted).to.equal(true);

    const unresolvedAfter = view.getUnresolvedSchemaRefs() as Array<[string, string]>;
    expect(unresolvedAfter).to.deep.equal([]);

    expect(view.getResolutionUriOfSchema('https://example.org/personinfo')).to.equal(
      'https://example.org/personinfo.yaml',
    );
  });
});

export {};
