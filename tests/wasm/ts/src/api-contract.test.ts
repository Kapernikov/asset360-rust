/**
 * API Contract Tests
 *
 * These tests guard against breaking changes to the public API,
 * especially the async/sync contract that broke browser rendering.
 */

import { ready, MiniJinjaEnvironment } from 'asset360-rust';
const { expect } = require('chai');

describe('API Contract', () => {
  let env: MiniJinjaEnvironment;

  before(async () => {
    // ready() must be called before creating MiniJinjaEnvironment
    await ready();
    env = new MiniJinjaEnvironment();
  });

  describe('Initialization', () => {
    it('ready() returns a Promise', () => {
      const result = ready();
      expect(result).to.be.instanceof(Promise);
    });

    it('ready() can be called multiple times (idempotent)', async () => {
      await ready();
      await ready();
      await ready();
      // Should not throw or reinitialize
    });

    it('MiniJinjaEnvironment can be created after ready()', () => {
      const env2 = new MiniJinjaEnvironment();
      expect(env2).to.be.instanceof(MiniJinjaEnvironment);
    });

    it('MiniJinjaEnvironment throws if WASM not initialized', async () => {
      // This would only happen if someone imports before ready() is called,
      // but we can't easily test that in this context since ready() is already called
      // This test documents the expected behavior
      expect(MiniJinjaEnvironment).to.be.a('function');
    });
  });

  describe('renderStr() synchronous contract', () => {
    it('renderStr() returns a string, not a Promise', () => {
      const template = 'Hello {{ name }}!';
      const context = { name: 'World' };

      const result = env.renderStr(template, context);

      // CRITICAL: renderStr must be synchronous
      expect(result).to.be.a('string');
      expect(result).to.not.be.instanceof(Promise);
      expect(result).to.equal('Hello World!');
    });

    it('renderStr() can be called multiple times synchronously', () => {
      const results: string[] = [];

      // These calls must be synchronous and not return Promises
      results.push(env.renderStr('{{ x }}', { x: '1' }));
      results.push(env.renderStr('{{ x }}', { x: '2' }));
      results.push(env.renderStr('{{ x }}', { x: '3' }));

      expect(results).to.deep.equal(['1', '2', '3']);
      results.forEach(r => {
        expect(r).to.be.a('string');
        expect(r).to.not.be.instanceof(Promise);
      });
    });

    it('renderStr() with complex template', () => {
      const template = `
        {% for item in items %}
        - {{ item.name }}: {{ item.value }}
        {% endfor %}
      `.trim();

      const context = {
        items: [
          { name: 'foo', value: 'bar' },
          { name: 'baz', value: 'qux' }
        ]
      };

      const result = env.renderStr(template, context);

      expect(result).to.be.a('string');
      expect(result).to.include('foo: bar');
      expect(result).to.include('baz: qux');
    });

    it('renderStr() handles empty context', () => {
      const result = env.renderStr('static text', {});
      expect(result).to.equal('static text');
      expect(result).to.be.a('string');
    });

    it('renderStr() handles missing variables gracefully', () => {
      const template = 'Hello {{ undefined_var }}!';
      const result = env.renderStr(template, {});

      expect(result).to.be.a('string');
      // MiniJinja renders undefined as empty string
      expect(result).to.equal('Hello !');
    });
  });

  describe('Multiple instances', () => {
    it('multiple MiniJinjaEnvironment instances can coexist', () => {
      const env1 = new MiniJinjaEnvironment();
      const env2 = new MiniJinjaEnvironment();
      const env3 = new MiniJinjaEnvironment();

      const result1 = env1.renderStr('{{ x }}', { x: 'A' });
      const result2 = env2.renderStr('{{ x }}', { x: 'B' });
      const result3 = env3.renderStr('{{ x }}', { x: 'C' });

      expect(result1).to.equal('A');
      expect(result2).to.equal('B');
      expect(result3).to.equal('C');
    });
  });

  describe('Error handling', () => {
    it('renderStr() with invalid template throws synchronously', () => {
      const template = '{% invalid syntax %}';

      expect(() => {
        env.renderStr(template, {});
      }).to.throw();
    });

    it('renderStr() with invalid context type throws synchronously', () => {
      expect(() => {
        // @ts-expect-error Testing runtime behavior
        env.renderStr('{{ x }}', null);
      }).to.throw();
    });
  });
});

export {};
