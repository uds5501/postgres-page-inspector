use crate::core::{Page, Tree};
use std::path::{Path};
use std::env;
use std::fs::{File, write};
use std::io::Write;
use handlebars::*;
use serde_json::value::Value;


struct ChildLevelPagesHelper;

struct SampleHelper;

struct IsArrayHelper;

impl HelperDef for ChildLevelPagesHelper {
    fn call<'reg: 'rc, 'rc>(&self,
                            h: &Helper,
                            _: &Handlebars,
                            _: &Context,
                            rc: &mut RenderContext,
                            out: &mut dyn Output) -> HelperResult {
        let param = h.param(0).unwrap();
        let pages: Vec<Page> = param.value().as_array().unwrap().iter()
            .map(|x| serde_json::from_value(x.clone()).unwrap()).collect();
        let child_pages = child_level_pages(pages);
        let pages_json: Value = serde_json::to_value(&child_pages).unwrap();
        out.write(pages_json.render().as_ref())?;
        Ok(())
    }
}

impl HelperDef for SampleHelper {
    fn call<'reg: 'rc, 'rc>(&self,
                            h: &Helper,
                            _: &Handlebars,
                            _: &Context,
                            rc: &mut RenderContext,
                            out: &mut dyn Output) -> HelperResult {
        let param = h.param(0).unwrap();
        println!("sample param - {:?}", param);

        out.write(param.render().as_ref())?;
        Ok(())
    }
}

impl HelperDef for IsArrayHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();

        // Check if the parameter is an array
        let is_array = match param.value() {
            JsonValue::Array(_) => true,
            _ => false,
        };

        if is_array {
            out.write("true").unwrap()
        } else {
            out.write("false").unwrap()
        }

        Ok(())
    }
}

// Implement your IsString helper
struct IsStringHelper;

impl HelperDef for IsStringHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper,
        _: &Handlebars,
        _: &Context,
        _: &mut RenderContext,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0).unwrap();

        // Check if the parameter is a string
        let is_string = match param.value() {
            JsonValue::String(_) => true,
            _ => false,
        };

        if is_string {
            out.write("true").unwrap();
        } else {
            out.write("false").unwrap();
        }

        Ok(())
    }
}

fn child_level_pages(pages: Vec<Page>) -> Vec<Page> {
    let mut child_pages: Vec<Page> = vec![];
    for page in pages {
        if page.is_leaf {
            break;
        }
        for item in page.items {
            if let Some(child) = item.child {
                child_pages.push(*child);
            }
        }
    }
    child_pages
}

pub fn render(tree: Tree) {
    let mut templates_dir = env::current_dir().unwrap();
    templates_dir = templates_dir.join("src").join("templates");
    let output_path = Path::new("output.html");
    let mut handlebars = Handlebars::new();


    let render_tree_file = templates_dir.join("render_tree.hbs");
    let render_page_file = templates_dir.join("render_page.hbs");
    let render_level_file = templates_dir.join("render_level.hbs");
    handlebars.register_template_file("render_tree", render_tree_file).unwrap();
    handlebars.register_template_file("render_page", render_page_file).unwrap();
    handlebars.register_template_file("render_level", render_level_file).unwrap();
    handlebars.register_helper("child_level_pages", Box::new(ChildLevelPagesHelper));
    handlebars.register_helper("sample", Box::new(SampleHelper));
    handlebars.register_helper("isArray", Box::new(IsArrayHelper));
    handlebars.register_helper("isString", Box::new(IsStringHelper));

    let mut map = serde_json::Map::new();
    map.insert("tree".to_string(), serde_json::to_value(&tree).unwrap());
    let rendered = handlebars.render("render_tree", &map).unwrap();
    let mut file = File::create(output_path).expect("Unable to create file");
    file.write_all(rendered.as_bytes()).expect("Unable to write data to file");
}